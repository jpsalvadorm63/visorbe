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
            select
                array_to_json(array_agg(row_to_json(ds))) longjson
            from (
                select
                    ${year} "year",
                    q.magnitude_id,
                    q.opoint_id,
                    array_agg(ds order by ds.datetime) as data
                from
                     (
                        select
                               magnitude_id,
                               opoint_id
                        from dashboard.magop_ids
                        where magnitude_id in ${magnitudes} and opoint_id in ${opoints}
                     ) q
                     left join
                     (
                         select
                            ds.magnitude_id m,
                            ds.opoint_id o,
                            ds.datetime,
                            case
                                when ds.magnitude_id in (3,10) then ds.avg12
                                when ds.magnitude_id in (1,24) then ds.avg24
                                when ds.magnitude_id = 24 then ds.avg8
                                else ds.avg1
                            end c,
                            case
                                when ds.magnitude_id in (3,10) then ds.min12
                                when ds.magnitude_id in (1,24) then ds.min24
                                when ds.magnitude_id = 24 then ds.min8
                                else ds.min1
                            end mn,
                            case
                                when ds.magnitude_id in (3,10) then ds.max12
                                when ds.magnitude_id in (1,24) then ds.max24
                                when ds.magnitude_id = 24 then ds.max8
                                else ds.max1
                            end mx,
                            case
                                when ds.magnitude_id in (1,3,6,8,10,24) then ds.aqi
                            end aqi,
                            case
                                when ds.magnitude_id in (1,3,6,8,10,24) then ds.iqca
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
                     ) ds on q.magnitude_id = ds.m and q.opoint_id = ds.o
                group by q.magnitude_id, q.opoint_id) ds
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
        String itvl  = params['itvl']
        def magn = params['magn'] as int
        if(magn == 81) magn = 82
        def year = params['year'] as int
        def month = params['month'] as int
        def dom = params['dom'] as int
        def sqlstr = """
            select
                array_to_json(array_agg(row_to_json(t))) longjson
            from
                (select
                    ${magn}::integer           magnitude_id,
                    case
                        when magnitude_id = 81 then sqrt(avg(rdatax)*avg(rdatax)+avg(rdatay)*avg(rdatay))::numeric(12,3)
                        when magnitude_id = 82 then atan2d(avg(rdatax),avg(rdatay))::numeric(12,3)
                        else avg(rdata)::numeric(12,3)
                    end rdata,
                    case
                        when magnitude_id = 81 then sqrt(min(rdatax)*min(rdatax)+min(rdatay)*min(rdatay))::numeric(12,3)
                        when magnitude_id = 82 then atan2d(min(rdatax),min(rdatay))::numeric(12,3)
                        else min(rdata)::numeric(12,3)
                    end rmin,
                    case
                        when magnitude_id = 81 then sqrt(max(rdatax)*max(rdatax)+max(rdatay)*max(rdatay))::numeric(12,3)
                        when magnitude_id = 82 then atan2d(max(rdatax),max(rdatay))::numeric(12,3)
                        else max(rdata)::numeric(12,3)
                    end rmax,
                    avg(aqi)::numeric(3)       aqi,
                    avg(iqca)::numeric(3)      iqca,
                    avg(rdatax)::numeric(12,3) rdatax,
                    avg(rdatay)::numeric(12,3) rdatay
                from analysis.api_magnitude_analysis('${itvl}','total',${magn},null,${year},${month},${dom})
                group by magnitude_id) t
        """
        def sqlconn = new Sql(dataSource)
        def longjson = null
        sqlconn.eachRow(sqlstr) { it ->
            longjson = (it.longjson != null)?it.longjson:null
        }
        render(status: 200, contentType: "application/json", text: (longjson != null)?longjson:'"no":"data"')
    }

    def analysisTotMagnitude4Opoint() {
        String itvl  = params['itvl']
        def magn = params['magn'] as int
        if(magn == 81) magn = 82
        def opoint = params['opoint'] as int
        def year = params['year'] as int
        def month = params['month'] as int
        def dom = params['dom'] as int
        def sqlstr = """
            select
                array_to_json(array_agg(row_to_json(t))) longjson
            from (select * 
                from analysis.api_magnitude_analysis('${itvl}','total',${magn},${opoint},${year},${month},${dom})) t
        """
        def sqlconn = new Sql(dataSource)
        def longjson = null
        sqlconn.eachRow(sqlstr) { it ->
            longjson = (it.longjson != null)?it.longjson:null
        }
        render(status: 200, contentType: "application/json", text: (longjson != null)?longjson:'"no":"data"')
    }

    def analysisTotMagnitudeXDate() {
        String itvl  = params['itvl']
        def magn = params['magn'] as int
        if(magn == 81) magn = 82
        def year = params['year'] as int
        def month = params['month'] as int
        def dom = params['dom'] as int
        def sqlstr = """
            select
                array_to_json(array_agg(row_to_json(t))) longjson
            from
                (select
                    datetime,
                    case
                        when magnitude_id = 81 then sqrt(avg(rdatax)*avg(rdatax)+avg(rdatay)*avg(rdatay))::numeric(12,3)
                        when magnitude_id = 82 then atan2d(avg(rdatax),avg(rdatay))::numeric(12,3)
                        else avg(rdata)::numeric(12,3)
                    end                       rdata,
                    case
                        when magnitude_id = 81 then sqrt(min(rdatax)*min(rdatax)+min(rdatay)*min(rdatay))::numeric(12,3)
                        when magnitude_id = 82 then atan2d(min(rdatax),min(rdatay))::numeric(12,3)
                        else min(rdata)::numeric(12,3)
                    end                        rmin,
                    case
                        when magnitude_id = 81 then sqrt(max(rdatax)*max(rdatax)+max(rdatay)*max(rdatay))::numeric(12,3)
                        when magnitude_id = 82 then atan2d(max(rdatax),max(rdatay))::numeric(12,3)
                        else max(rdata)::numeric(12,3)
                    end                        rmax,
                    avg(aqi)::numeric(3)       aqi,
                    avg(iqca)::numeric(3)      iqca,
                    avg(rdatax)::numeric(12,3) rdatax,
                    avg(rdatay)::numeric(12,3) rdatay
                from analysis.api_magnitude_analysis('${itvl}','per day',${magn},null,${year},${month},${dom})                
                group by datetime, magnitude_id) t
        """
        println sqlstr
        def sqlconn = new Sql(dataSource)
        def longjson = null
        sqlconn.eachRow(sqlstr) { it ->
            longjson = (it.longjson != null)?it.longjson:null
        }
        render(status: 200, contentType: "application/json", text: (longjson != null)?longjson:'"no":"data"')
    }

    def analysisTotMagnitudeXDate4Opoint() {
        String itvl  = params['itvl']
        def magn = params['magn'] as int
        if(magn == 81) magn = 82
        def opoint = params['opoint'] as int
        def year = params['year'] as int
        def month = params['month'] as int
        def dom = params['dom'] as int
        def sqlstr = """
            select
                array_to_json(array_agg(row_to_json(t))) longjson
            from (select
                    datetime,
                    magnitude_id,
                    opoint_id,
                    case
                        when magnitude_id = 81 then sqrt(rdatax*rdatax+rdatay*rdatay)::numeric(12,3)
                        when magnitude_id = 82 then atan2d(rdatax,rdatay)::numeric(12,3)
                        else rdata::numeric(12,3)
                    end                       rdata,
                    rmin,
                    rmax,
                    aqi::numeric(3)       aqi,
                    iqca::numeric(3)      iqca,
                    rdatax::numeric(12,3) rdatax,
                    rdatay::numeric(12,3) rdatay 
                from analysis.api_magnitude_analysis('${itvl}','per day',${magn},${opoint},${year},${month},${dom})) t
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
