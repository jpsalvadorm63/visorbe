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
