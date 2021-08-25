package visorbe

import grails.rest.*
import grails.converters.*
import grails.core.support.GrailsConfigurationAware
import grails.config.Config
//import groovy.json.JsonBuilder
//import groovy.json.StreamingJsonBuilder
import groovy.sql.Sql
//import java.sql.Timestamp
//import grails.converters.JSON
//import static org.springframework.http.HttpStatus.OK

class AnalyticsController implements GrailsConfigurationAware {
	static responseFormats = ['json', 'xml']
    static scope = "prototype"
    String csvMimeType
    String encoding
    def dataSource
	
    def index() { }

    /**
     * http://localhost:9090/analytics/period?year=2021&month=1&dom=1&timelap=7+days&row=per+hour&magnitudes=(1,3,6,8,10,14)&opoints=(3,5)
     */
    def qryPeriod() {
        String magnitudes=params['magnitudes']?(params['magnitudes']):null
        String opoints=params['opoints']?(params['opoints']):null
        Integer year = params['year']?(params['year'] as Integer):null
        Integer month = params['month']?(params['month'] as Integer):null
        Integer dom = params['dom']?(params['dom'] as Integer):null
        String timelap = params['timelap']?(params['timelap']):null
        String row = params['row']?(params['row']):null
        String longjson = null
        String
        String sqlstr = """
            select array_to_json(array_agg(row_to_json(ds))) longjson from(
                select
                    ds.magnitude_id,
                    ds.opoint_id,
                    array_agg(ds.datetime order by ds.datetime) datetime,
                    case
                        when ds.magnitude_id in (3,10) then array_agg(ds.avg12 order by ds.datetime)
                        when ds.magnitude_id in (1,24) then array_agg(ds.avg24 order by ds.datetime)
                        when ds.magnitude_id = 24 then array_agg(ds.avg8 order by ds.datetime)
                        else array_agg(ds.avg1 order by ds.datetime)
                    end concentration,
                    case
                        when ds.magnitude_id in (3,10) then array_agg(ds.min12 order by ds.datetime)
                        when ds.magnitude_id in (1,24) then array_agg(ds.min24 order by ds.datetime)
                        when ds.magnitude_id = 24 then array_agg(ds.min8 order by ds.datetime)
                        else array_agg(ds.min1 order by ds.datetime)
                    end min_concentration,
                    case
                        when ds.magnitude_id in (3,10) then array_agg(ds.max12 order by ds.datetime)
                        when ds.magnitude_id in (1,24) then array_agg(ds.max24 order by ds.datetime)
                        when ds.magnitude_id = 24 then array_agg(ds.max8 order by ds.datetime)
                        else array_agg(ds.max1 order by ds.datetime)
                    end max_concentration,
                    case
                        when ds.magnitude_id in (1,3,6,8,10,24) then array_agg(ds.aqi order by ds.datetime)
                    end aqi,
                    case
                        when ds.magnitude_id in (1,3,6,8,10,24) then array_agg(ds.iqca order by ds.datetime)
                    end iqca
                from dashboard.api_dataseries(
                    '${timelap}',
                    '${row}',
                    null,
                    null,
                    ${year},
                    ${month},
                    ${dom},
                    0
                ) ds
                where ds.magnitude_id in ${magnitudes} and ds.opoint_id in ${opoints} 
                group by ds.magnitude_id,ds.opoint_id
            ) ds
        """
        def sqlconn = new Sql(dataSource)
        sqlconn.eachRow(sqlstr) { it ->
            longjson = (it.longjson != null)?it.longjson:null
        }
        render(status: 200, contentType: "application/json", text: (longjson != null)?longjson:'{"no":"data"}')
    }

    def analysisForecasting() {
        String sqlstr = "select longjson from analysis.DashboardData where description = 'forecasting'"
        def sqlconn = new Sql(dataSource)
        def longjson = null
        sqlconn.eachRow(sqlstr) { it ->
            longjson = (it.longjson != null)?it.longjson:null
        }

        render(status: 200, contentType: "application/json", text: (longjson != null)?longjson:'"no":"data"')
    }

    def analysisLastData() {
        String itvl  = params['itvl']?params['itvl']:'L24H'
        String sqlstr = "select longjson from analysis.DashboardData where description = '${itvl}'"
        def sqlconn = new Sql(dataSource)
        def longjson = null
        sqlconn.eachRow(sqlstr) { it ->
            longjson = (it.longjson != null)?it.longjson:null
        }

        render(status: 200, contentType: "application/json", text: (longjson != null)?longjson:'"no":"data"')
    }

    def analysisTotMagnitudeDate() {
        String itvl  = params['itvl']?params['itvl']:'null'
        Integer magn = params['magn']?(params['magn'] as Integer):null
        if(magn == 81) magn = 82
        Integer opoint = params['opoint']?(params['opoint'] as Integer):null
        Integer year = params['year']?(params['year'] as Integer):null
        Integer month = params['month']?((params['month'] as Integer)):null
        Integer dom = params['dom']?((params['dom'] as Integer)):null
        Integer hour = params['hour']?((params['hour'] as Integer)):0
        def sqlstr = """
            select
                array_to_json(array_agg(row_to_json(t))) longjson
            from
            ( select
                * 
                from analysis.analysis_tot_per_magnitude_date(
                    ${itvl},
                    ${magn},
                    ${opoint},
                    ${year},
                    ${month},
                    ${dom},
                    ${hour}
                )  order by magnitude_id, datetime ) t
        """

        def sqlconn = new Sql(dataSource)
        def longjson = null
        sqlconn.eachRow(sqlstr) { it ->
            longjson = (it.longjson != null)?it.longjson:null
        }

        render(status: 200, contentType: "application/json", text: (longjson != null)?longjson:'"no":"data"')
    }

    def analysisTotMagnitude() {
        String itvl  = params['itvl']?params['itvl']:'null'
        Integer magn = params['magn']?(params['magn'] as Integer):null
        if(magn == 81) magn = 82
        Integer opoint = params['opoint']?(params['opoint'] as Integer):null
        Integer year = params['year']?(params['year'] as Integer):null
        Integer month = params['month']?((params['month'] as Integer)):null
        Integer dom = params['dom']?((params['dom'] as Integer)):null
        Integer hour = params['hour']?((params['hour'] as Integer)):0
        def sqlstr = """
            select
                array_to_json(array_agg(row_to_json(t))) longjson
            from
            ( select
                * 
                from analysis.analysis_tot_per_magnitude(
                    ${itvl},
                    ${magn},
                    ${opoint},
                    ${year},
                    ${month},
                    ${dom},
                    ${hour}
                )) t
        """
        def sqlconn = new Sql(dataSource)
        def longjson = null
        sqlconn.eachRow(sqlstr) { it ->
            longjson = (it.longjson != null)?it.longjson:null
        }
        render(status: 200, contentType: "application/json", text: (longjson != null)?longjson:'"no":"data"')
    }

    def analysisTot() {
        String itvl  = params['itvl']?params['itvl']:'null'
        Integer magn = params['magn']?(params['magn'] as Integer):null
        if(magn == 81) magn = 82
        Integer opoint = params['opoint']?(params['opoint'] as Integer):null
        Integer year = params['year']?(params['year'] as Integer):null
        Integer month = params['month']?((params['month'] as Integer)):null
        Integer dom = params['dom']?((params['dom'] as Integer)):null
        Integer hour = params['hour']?((params['hour'] as Integer)):0
        def sqlstr = """
            select
                array_to_json(array_agg(row_to_json(t))) longjson
            from
            ( select
                * 
                from analysis.analysis_tot(
                    ${itvl},
                    null,
                    ${magn},
                    ${opoint},
                    ${year},
                    ${month},
                    ${dom},
                    ${hour}
                )) t
        """

        def sqlconn = new Sql(dataSource)
        def longjson = null
        sqlconn.eachRow(sqlstr) { it ->
            longjson = (it.longjson != null)?it.longjson:null
        }

        render(status: 200, contentType: "application/json", text: (longjson != null)?longjson:'"no":"data"')
    }

    @Override
    void setConfiguration(Config co) {
        csvMimeType = co.getProperty('grails.mime.types.csv', String, 'text/csv')
        encoding = co.getProperty('grails.converters.encoding', String, 'UTF-8')
    }
}
