package visorbe

import grails.config.Config
import grails.core.support.GrailsConfigurationAware
//import grails.rest.*
import grails.converters.*
import groovy.sql.Sql
import groovy.json.JsonOutput
import static org.springframework.http.HttpStatus.OK

class DataseriesController implements GrailsConfigurationAware {
    static scope = "prototype"
    String csvMimeType
    String encoding
    static responseFormats = ['json', 'html']
    def dataSource
    def modelService

    def index() { }

    // other functions
    static def createDate(Integer year, Integer month, Integer dom, Integer hour=0) {
        def myDate = null
        if(year != null && month != null && dom != null && hour != null) {
            try {
                myDate = new Date().copyWith(year: year, month: month - 1 , dayOfMonth: dom, hourOfDay: hour, minute: 0, second: 0)
            } catch(ignored) {
                myDate = null
            }
        }
        return myDate
    }

    static def getDataDescription(rowc, lang='en') {
        return [
            'C24h':'Each hourly concentration data is calculated as the average of the previous 24 hours. For exmple: the average at 12H00 is the average between 12H00 (previous day) and 11H59',
            'C8h':'Each hourly concentration data is calculated as the average of the previous 8 hours. For exmple: the average at 12H00 is the average between 04H00 and 11H59',
            'C1h':'Each hourly concentration data is calculated as the average of the next 60 minutes. For example: the average at 12H00 is the average between 12H00 and 12H59',
            'P1hP':'The one hour average is computed as the average of the next 60 minutes. For example: the average at 12H00 is the average between 12H00 and 12H59',
            'Mx10m':'The max ten minutes average in one hour is calculated after partitioning one hour in 6 parts (ten minutes each), then getting the average for each partition, and selecting the highest',
            'Ac1h':'The sum of every minute in one hour'
        ][rowc]
    }

    static def getRowDescription(row, lang='en') {
        return [
            'per hour':'Each row shows hourly data, depending on original raw data, may calculate the average, maximun, minimun, sum and/or max 10 minutes average from the original raw data.',
            'per day':'Each hourly data row calculates agregated data based on hourly data per day',
            'per month':'Each hourly data row calculates agregated data based on hourly data per month',
            'per year':'Each hourly data row calculates agregated data based on hourly data year',
            'total':'Each hourly data row calculates agregated data based on hourly data'
        ][row]
    }

    static String toStr(Number x, int dec=0) { (x instanceof Number) ? (x as Double).round(dec) : '' }

    static String setDecimalP(String x, decimalp='.') {(decimalp=='.')?x:x?.replace('.', decimalp)}

//    def mimapa() {
//        def result = '{"type":"FeatureCollection","crs":{"type":"name","properties":{"name":"urn:ogc:def:crs:EPSG::32717"}},"features":[{"type":"Feature","geometry": {"type":"MultiPolygon","coordinates":[[[[728278.848236808,9648755.69121868],[728278.848236808,9648755.47442728],[728242.290369795,9648738.99074618],[728204.88,9648784.33],[728210.85,9648800.24],[728278.848236808,9648755.69121868]]]]},"properties": {"pid":"0203","abbreviation":"ARG","parroquia":"LA ARGELIA"}}]}'
//        render(status: 200, contentType: "application/json", text: result)
//    }

    /**
     *
     * @return
     */
    def qryDsCSV() {
        // service call sample
        // http://www.aqvisor.net:9090/visorbe-0.2/dshbrd/csvdn?lang=en&magnitude=1,3,6&opoints=1,3,8&year=2021&month=3&dom=25&hour=0&itvl=1+hour&mode=remmaq&row=per+hour&show=data&complete=true&decimalp=false
        def show = params.show
        def outs = response.outputStream
        response.status = OK.value()
        response.contentType = "&#36;{csvMimeType};charset=&#36;{encoding}"
        if(show == 'help') {
            response.setHeader "Content-disposition", "attachment; filename=REMMAQ_HELP_dshbrd_csvdn.md"
            outs << "# Endpoint: http:/www.aqvisor.net:9090/dshbrd/csvdn?...(params)\n".bytes
            outs << "## Params:\n".bytes
            outs << "* **lang** .- language of report: **en** for english, **es** for spanish. For example: `http://www.aqvisor.net:9090/dshbrd/csvdn?lang=en`\n".bytes
            outs << "* **magnitdes** .- comma separated list of magnitude codes, according the next list:\n".bytes
            for(def mg in modelService.magnitude_schemas) {
                outs << "	* `${mg.magnitude_id}` = `${mg.magnitude_name['en']}, spanish: `${mg.magnitude_name['es']}`\n"
            }
            outs << "* **opoints** .- somma separated list of station codes, according the next list:\n".bytes
            for(def op in modelService.opoints2.sort{it.id}) {
                outs << "	* `${op?.id}` = (`${op?.pid}`) `${op.opoint_name}`\n"
            }
            outs << "* **year** .- reference date year: 2004, 2005, 2006 ... to curren year\n".bytes
            outs << "* **month** .- reference date mont: 1 for January, 2 for February,.... 12 fro December\n".bytes
            outs << "* **dom** .- reference date day of month: 1, 2, 3, ... , 31\n".bytes
            outs << "* **hour** .- reference date hour: 0, 1, 2, 3, ... , 0\n".bytes
            outs << "* **itvl** .- interval of time according to next list:\n".bytes
            outs << "  * `8+hours` .- 8 horas after reference date\n".bytes
            outs << "  * `24+hours` .- 24 horas after reference date\n".bytes
            outs << "  * `7+days .- 7 days after the reference date`\n".bytes
            outs << "  * `1+month .- 1 month after the reference date`\n".bytes
            outs << "  * `1+year .- 1 year after the reference date`\n".bytes
            outs << "* **row** .- type of agregate data in each row, accordin to the next list\n".bytes
            outs << "  * `per+hour` .- each row represents hourly data\n".bytes
            outs << "  * `per+day` .- each row represents dayly data\n".bytes
            outs << "  * `per+month` .- each row represents monthly data\n".bytes
            outs << "  * `per+year` .- each row represents yearly data\n".bytes
            outs << "* **mode** .- if this parameter get the valu of `nasa`, the magnitudes names will get the NASA names\n".bytes
            outs << "* **show** .- which information is downloaded, according next list:\n".bytes
            outs << "  * `data` .- CSV file with data\n".bytes
            outs << "  * `metadata` .- a markdown file containing themetadata corresponding to CSV file\n".bytes
            outs << "  * `help` .- a markdown file with this help info\n".bytes
            outs << "* **complete** .- true, all columns. false: few columns\n".bytes
            outs << "* **decimalp** .- true, a numbre use '.' on decimal points. false, ',' for decimal numbers. false: few columns\n".bytes
            outs << "* **sample**\n".bytes
            outs << "`http://www.aqvisor.net:9090/visorbe-0.2/dshbrd/csvdn?lang=en&magnitude=14&opoint=null&year=2021&month=3&dom=25&hour=0&itvl=24+hours&mode=remmaq&row=per+hour&show=data&complete=true&decimalp=false`\n"
        } else {
            def getMagnitudeSchema = modelService.getMagnitudeSchema
            // params
            String lang = params.lang?params.lang:'en'
            def magnitudes = (params.magnitudes == null || params.magnitudes?.toLowerCase() == 'null')?null:params.magnitudes.split(',')?.collect {it as Integer}
            def opoints = (params.opoints == null || params.opoints?.toLowerCase() == 'null')?'null':params.opoints
            def year = params.year?(params.year as Integer):null
            def month = params.month?(params.month as Integer):null
            def dom = params.dom?(params.dom as Integer):null
            def hour = params.dom?(params.hour as Integer):0
            def itvl = params.itvl?params.itvl:null
            def row = params.row?:'per hour'
            def rowDesc = [
                'per hour':(lang == 'en')?'Hourly':'Horario',
                'per day':(lang == 'en')?'Daily':'Diario',
                'per month':(lang == 'en')?'Monthly':'Mmensual',
                'per year':(lang == 'en')?'Yearly':'Anual',
                'total':'TOTAL'
            ][row]
            def mode = (params.mode == 'nasa')?'nasa':'remmaq'
            def decimalp = (params.decimalp == 'true')?'.':','
            def complete =  (params.complete == 'true')

            //output file names
            def date = createDate(year, month, dom, hour)
            String filename = "REMMAQ_${date?date.format('yyyyMMdd-HHmm-')+magnitudes?.join('-'):'ERROR'}"
            String filenamemd = "${filename}.md"
            String filenamecsv = "${filename}.csv"
            // input params erors
            def errors = ""
            if(magnitudes == null) errors = errors + '  * Magnitude param is not valid (magnitude param)\n'
            if(date == null) errors = errors + '  * wrong date parameters (year, month, dom, hour params)\n'
            if(itvl == null) errors = errors + '  * interval must be defined (`itvl` param)\n'
            if(!['per hour', 'per day', 'per month', 'per year', 'total'].contains(row)) errors = errors + '  * unknown row code (`row` param)\n'
            for(Integer magnitude_id in magnitudes) {
                def magnitude_schema = modelService.magnitude_schemas.find {it.magnitude_id  == magnitude_id }
                if(magnitude_schema == null) errors = errors + "  * Unknown magnitude code (magnitude param): ${magnitude_id}\n"
            }
            if(show == 'metadata') {
                response.setHeader "Content-disposition", "attachment; filename=${filenamemd}"
                outs << "# REMMAQ|Visor System, Dataseries Downloader\n\n".bytes
                if(errors?.length() > 0) {
                    outs << "## Params\n\n".bytes
                    outs << "### **Errors**\n\n".bytes
                    outs << "${errors}\n\n".bytes
                } else {
                    outs << "## Params\n\n".bytes
                    outs << "### Magnitudes\n\n".bytes
                    outs << "  * Date and interval: from ${date.format('yyyy.MM.dd-HH:mm')}, ${itvl}. ${(lang == 'en')?'Each data row is ':'Each fila representa un dato '} ${rowDesc} ${(lang == 'en')?' in the interval':' En el intervalo '}\n\n".bytes
                    for(Integer magnitude_id in magnitudes) {
                        def magnitude_schema = modelService.magnitude_schemas.find {it.magnitude_id  == magnitude_id }
                        outs << "\n#### **`${magnitude_schema.magnitude_name['en']}, id: ${magnitude_id}`**".bytes
                        if(magnitude_schema.nasa_name != null)
                            outs << ", Nasa name: `${magnitude_schema.nasa_name}` \n\n".bytes
                        else outs << "\n\n".bytes
                        outs << "  * Magnitude name in english: **`${magnitude_schema.magnitude_name['en']}`**\n\n".bytes
                        outs << "  * Magnitude name in spanish: **`${magnitude_schema.magnitude_name['es']}`**\n\n".bytes
                        outs << "  * NASA magnitude name: **`${magnitude_schema.nasa_name?magnitude_schema.nasa_name:'n/a'}`**\n\n".bytes
                        outs << "  * Units: **`${magnitude_schema.unit?magnitude_schema.unit:'n/a'}`**\n\n".bytes
                        outs << "  * Base data: **${magnitude_schema.DATA.colDescription('en')[0]}**, ${magnitude_schema.DATA.colDescription('en')[1]}, ${getDataDescription(magnitude_schema.DATA.colDescription('en')[0])}\n\n".bytes
                        outs << "  * Row width: **${row}**, ${getRowDescription(row)}\n\n".bytes
                    }
                    outs << "## Columns\n\n".bytes
                    outs << "  * **row** .- type of data, `hour' hourly data, `aggr` aggregated data\n\n".bytes
                    outs << "### Magnituide and date time Columns\n\n".bytes
                    outs << "  * **magnitude** .- magnitude name\n\n".bytes
                    outs << "  * **station** .- station name\n\n".bytes
                    outs << "  * **datetime** .- local date and time\n\n".bytes
                    outs << "  * **utcdatetime** .- equivalent UTC date and time\n\n".bytes
                    outs << "### ${row!='per hour'?(rowDesc + ' aggregated '):''} Columns\n\n".bytes
                    outs << "  * **data_1h** .- ${row!='per hour'?(row + ' aggregated based on '):''}hourly data${row!='per hour'?(' groupped on Magnitude/station/datetime '):''}\n\n".bytes
                    if(complete) {
                        outs << "  * **health_1h** .- health Index for 1 hour concentration\n\n".bytes
                        outs << "  * **data_1h_X** .- x axis data component (for Wind magnitude)\n\n".bytes
                        outs << "  * **data_1h_Y** .- y axis data component (for Wind magnitude)\n\n".bytes
                        outs << "  * **data_1h_min** .- minimum\n\n".bytes
                        outs << "  * **data_1h_max** .- maximum\n\n".bytes
                        outs << "  * **data_1h_forecasting** .- forecasting data component\n\n".bytes
                        outs << "  * **data_1h_forecastingX** .- x axis forecasting data component (for Wind magnitude)\n\n".bytes
                        outs << "  * **data_1h_forecastingY** .- x axis data forecasting component (for Wind magnitude)\n\n".bytes
                    }
                    outs << "### Data Concentration Columns\n\n".bytes
                    outs << "  * **concentration** .- Type of concentration: 1 hour, 8 hours, 24 hour, as detailed next:\n".bytes
                    for(Integer magnitude_id in magnitudes) {
                        def magnitude_schema = modelService.magnitude_schemas.find { it.magnitude_id == magnitude_id }
                        if(magnitude_schema !=null) {
                            outs << "    - **${magnitude_schema?.magnitude_name[lang]}** .- ${magnitude_schema?.DATA?.colDescription?.call(lang)?.getAt(1)}\n\n".bytes
                        }
                    }
                    outs << "  * **conct_data** .- concentration data\n\n".bytes
                    if(complete) {
                        outs << "  * **conct_data_health** .- concentration health indicator\n\n".bytes
                        outs << "  * **conct_data_x** .- x axis concentration data\n\n".bytes
                        outs << "  * **conct_data_yx** .- y axis concentration data\n\n".bytes
                        outs << "  * **conct_data_min** .- min concentration data\n\n".bytes
                        outs << "  * **conct_data_max** .- max concentration data\n\n".bytes
                        outs << "  * **conct_forecastingx** .- forecasting concentration data\n\n".bytes
                        outs << "  * **conct_forecasting_minx** .- min forecasting concentration data\n\n".bytes
                        outs << "  * **conct_forecasting_max** .- max forecasting concentration data\n\n".bytes
                    }
                    outs << "### Indexces \n\n".bytes
                    outs << "  * **IQCA** .- Quito Air Quality Index\n\n".bytes
                    outs << "  * **AQI** .- Air quality international (USA) index\n\n".bytes
                }
            }
            else
            {
                response.setHeader "Content-disposition", "attachment; filename=${filenamecsv}"
                if(complete) {
                    outs << "row;magnitude;station;datetime;utcdatetime;".bytes
                    outs << "_1;data_1h;health_1h;data_1h_X;data_1h_Y;data_1h_min;data_1h_max;data_1h_forecasting;".bytes
                    outs << "data_1h_forecastingX;data_1h_forecastingY;".bytes
                    outs << "_2;concentration;conct_data;conct_data_health;conct_data_x;conct_data_y;conct_data_min;".bytes
                    outs << "conct_data_max;conct_forecasting;conct_forecasting_min;".bytes
                    outs << "conct_forecasting_max;_3;IQCA;AQI".bytes
                } else {
                    outs << "row;magnitude;station;datetime;utcdatetime;_1;data_1h;_2;concentration;conct_data;_3;IQCA;AQI".bytes
                }
                outs << "\n".bytes
                def sqlconn = new Sql(dataSource)
                int n = 0
                for(Integer magnitude_id in magnitudes) {
                    String mysql = modelService.getSql4_1mg (itvl, row, magnitude_id, opoints, year, month, dom, hour)
                    sqlconn.eachRow(mysql) { rr ->
                        def magnitude_schema = getMagnitudeSchema(rr?.magnitude_id)
                        def magnitude_name = magnitude_schema?.magnitude_name?.getAt(lang)?.replace('ó','o')?.replace('á','a')?.replace('é','e')?.replace('í','i')?.replace('ú','u')
                        def rrfc = [
                            isHourlyData:rr?.isHourlyData,
                            value1:rr?.fc1,
                            value1max:rr?.fc1max,
                            value8h:rr?.fc8,
                            value8hmax:rr?.fc8max,
                            value24h:rr?.fc24,
                            value24hmax:rr?.fc24max
                        ]
                        outs << "${(rr?.isHourlyData == true?"hour":"aggr")};".bytes
                        outs << "${(mode=='nasa' && magnitude_schema?.nasa_name)?magnitude_schema.nasa_name:magnitude_name};".bytes
                        outs << "${modelService.getOpoint2(rr?.opoint_id)?.opoint_name};".bytes
                        outs << "${rr?.datetime};".split(' ').join('T').bytes
                        outs << "${rr?.utcdatetime};".bytes
                        outs << "|;".bytes
                        String c1h = toStr(rr?.value1, 3)
                        outs << "${setDecimalP(c1h, decimalp)};".bytes
                        if(complete) {
                            def c1h_health = (c1h!='')?magnitude_schema?.c1h_health?.call(c1h as Double):null
                            outs << "${(c1h_health!=null)?c1h_health:''};".bytes
                            outs << "${setDecimalP(toStr(rr?.value1x, 3), decimalp)};".bytes
                            outs << "${setDecimalP(toStr(rr?.value1y, 3), decimalp)};".bytes
                            outs << "${setDecimalP(toStr(rr?.value1min, 3), decimalp)};".bytes
                            outs << "${setDecimalP(toStr(rr?.value1max, 3), decimalp)};".bytes
                            outs << "${setDecimalP(toStr(rr?.fc1, 3), decimalp)};".bytes
                            outs << "${setDecimalP(toStr(rr?.fc1x, 3), decimalp)};".bytes
                            outs << "${setDecimalP(toStr(rr?.fc1y, 3), decimalp)};".bytes
                        }
                        outs << "|;".bytes
                        if([14,6].contains(magnitude_id)) {
                            outs << "${(lang == 'es')?'8 horas':'8 horas'};".bytes
                            def c8h = toStr(rr?.value8h, 3)
                            outs << "${setDecimalP(c8h, decimalp)};".bytes
                            if(complete) {
                                def c8h_health = (c8h!='')?magnitude_schema?.c8h_health?.call(c8h as Double):null
                                outs << "${(c8h_health!=null)?c8h_health:''};;;".bytes
                                outs << "${setDecimalP(toStr(rr?.value8min,3), decimalp)};".bytes
                                outs << "${setDecimalP(toStr(rr?.value8max,3), decimalp)};".bytes
                                outs << "${setDecimalP(toStr(rr?.fc8,3), decimalp)};".bytes
                                outs << "${setDecimalP(toStr(rr?.fc8min,3), decimalp)};".bytes
                                outs << "${setDecimalP(toStr(rr?.fc8max,3), decimalp)};".bytes
                            }
                        } else
                        if([1,3,10].contains(magnitude_id)) {
                            if(magnitude_id == 1)
                                outs << "${(lang == 'es')?'24 horas':'24 hours'};".bytes
                            else
                                outs << "${(lang == 'es')?'12 horas':'12 hours'};".bytes
                            def c24h = toStr(rr?.value24h,3)
                            outs << "${setDecimalP(c24h, decimalp)};".bytes
                            if(complete) {
                                def c24h_health = (c24h!='')?magnitude_schema?.c24h_health?.call(c24h as Double):null
                                outs << "${(c24h_health!=null)?c24h_health:''};;;".bytes
                                outs << "${setDecimalP(toStr(rr?.value24min,3), decimalp)};".bytes
                                outs << "${setDecimalP(toStr(rr?.value24max,3), decimalp)};".bytes
                                outs << "${setDecimalP(toStr(rr?.fc24,3), decimalp)};".bytes
                                outs << "${setDecimalP(toStr(rr?.fc24min,3), decimalp)};".bytes
                                outs << "${setDecimalP(toStr(rr?.fc24max,3), decimalp)};".bytes
                            }
                        } else {
                            outs << "${(lang == 'es')?'1 hora':'1 hour'};".bytes
                            outs << "${setDecimalP(c1h, decimalp)};".bytes
                            if(complete) {
                                def c1h_health = (c1h!='')?magnitude_schema?.c1h_health?.call(c1h as Double):null
                                outs << "${(c1h_health!=null)?c1h_health:''};".bytes
                                outs << "${setDecimalP(toStr(rr?.value1x, 3), decimalp)};".bytes
                                outs << "${setDecimalP(toStr(rr?.value1y, 3), decimalp)};".bytes
                                outs << "${setDecimalP(toStr(rr?.value1min, 3), decimalp)};".bytes
                                outs << "${setDecimalP(toStr(rr?.value1max, 3), decimalp)};".bytes
                                outs << "${setDecimalP(toStr(rr?.fc1, 3), decimalp)};".bytes
                                outs << "${setDecimalP(toStr(rr?.fc1x, 3), decimalp)};".bytes
                                outs << "${setDecimalP(toStr(rr?.fc1y, 3), decimalp)};".bytes
                            }
                        }
                        outs << "|;".bytes
                        outs << "${setDecimalP(toStr(rr?.iqca, 0), decimalp)};".bytes
                        outs << "${setDecimalP(toStr(rr?.aqi, 0), decimalp)};".bytes
                        outs << "\n"
                        n++
                        if(n==1024) {
                            outs.flush()
                            n=0
                        }
                    }
                }
            }
        }
        outs.flush()
        outs.close()
    }

    def queryToArray(sqlconn, query, boolean isHourlyData) {

        def getMagnitudeSchema = modelService.getMagnitudeSchema
        def rs = []
        sqlconn.eachRow(query) { rrs ->
            def rr = rrs.toRowResult()
            rr.remove('ishourlydata')
            rr.setProperty('isHourlyData', isHourlyData)

            def magnitude_schema = getMagnitudeSchema(rr?.magnitude_id)

            def c1h = rr?.value1 ? Math.round(rr?.value1 * 10.0) / 10.0 : null
            def c1h_health = magnitude_schema?.c1h_health?.call(c1h)
            def c1hx = rr?.value1x ? Math.round(rr?.value1x * 10.0) / 10.0 : null
            def c1hy = rr?.value1y ? Math.round(rr?.value1y * 10.0) / 10.0 : null
            def c1hmin = rr?.value1min ? Math.round(rr?.value1min * 10.0) / 10.0 : null
            def c1hmax = rr?.value1max ? Math.round(rr?.value1max * 10.0) / 10.0 : null

            def c8h = (rr?.value8h != null) ? Math.round(rr?.value8h * 10.0) / 10.0 : null
            def c8h_health = (c8h != null) ? magnitude_schema?.c8h_health?.call(c8h) : null
            def c8hmin = rr?.value8min ? Math.round(rr?.value8min * 10.0) / 10.0 : null
            def c8hmax = rr?.value8max ? Math.round(rr?.value8max * 10.0) / 10.0 : null

            def c24h = (rr?.value24h != null) ? Math.round(rr?.value24h * 10.0) / 10.0 : null
            def c24h_health = (c24h != null) ? magnitude_schema?.c24h_health?.call(c24h) : null
            def c24hmin = rr?.value24min ? Math.round(rr?.value24min * 10.0) / 10.0 : null
            def c24hmax = rr?.value24max ? Math.round(rr?.value24max * 10.0) / 10.0 : null

            def iqca = magnitude_schema?.IQCA?.value?.call(rr)
            def aqi = magnitude_schema?.AQI?.value?.call(rr)

            rs << [
                    magnitude_id: rr?.magnitude_id,
                    opoint_id   : rr?.opoint_id,
                    row         : (rr?.isHourlyData == true) ? "hour" : "aggr",
                    datetime    : rr?.datetime,

                    c1h         : (c1h != null) ? c1h : null,
                    c1h_health  : (c1h_health != null) ? c1h_health : null,
                    c1hx        : (c1hx != null) ? c1hx : null,
                    c1hy        : (c1hy != null) ? c1hy : null,
                    c1hmin      : (c1hmin != null) ? c1hmin : null,
                    c1hmax      : (c1hmax != null) ? c1hmax : null,

                    c8h         : (c8h != null) ? c8h : null,
                    c8h_health  : (c8h_health != null) ? c8h_health : null,
                    c8hmin      : (c8hmin != null) ? c8hmin : null,
                    c8hmax      : (c8hmax != null) ? c8hmax : null,

                    c24h        : (c24h != null) ? c24h : null,
                    c24h_health : (c24h_health != null) ? c24h_health : null,
                    c24hmin     : (c24hmin != null) ? c24hmin : null,
                    c24hmax     : (c24hmax != null) ? c24hmax : null,

                    iqca        : (iqca != null) ? iqca : null,
                    aqi         : (aqi != null) ? aqi : null
                ]
        }
        return rs
    }

    def qryDsJSON() {
        def sqlconn = new Sql(dataSource)
        def ds = [
                'AQ': [
                        'L24H':[
                                max: queryToArray(sqlconn, "select * from dashboard.selectLastData('AQ', '-24 hours', 'max')", true),
                                avg_per_magnitude: queryToArray(sqlconn, "select * from dashboard.selectLastData('AQ', '-24 hours', 'avg_per_magnitude')", true),
                                min_per_magnitude: queryToArray(sqlconn, "select * from dashboard.selectLastData('AQ', '-24 hours', 'min_per_magnitude')", true),
                                max_per_magnitude: queryToArray(sqlconn, "select * from dashboard.selectLastData('AQ', '-24 hours', 'max_per_magnitude')", true),
                                max_per_opoint: queryToArray(sqlconn, "select * from dashboard.selectLastData('AQ', '-24 hours', 'max_per_opoint')", true)
                        ],
                        'L7D':[
                                max: queryToArray(sqlconn, "select * from dashboard.selectLastData('AQ', '-7 days', 'max')", false),
                                avg_per_magnitude: queryToArray(sqlconn, "select * from dashboard.selectLastData('AQ', '-7 days', 'avg_per_magnitude')", false),
                                min_per_magnitude: queryToArray(sqlconn, "select * from dashboard.selectLastData('AQ', '-7 days', 'min_per_magnitude')", false),
                                max_per_magnitude: queryToArray(sqlconn, "select * from dashboard.selectLastData('AQ', '-7 days', 'max_per_magnitude')", false),
                                max_per_opoint: queryToArray(sqlconn, "select * from dashboard.selectLastData('AQ', '-7 days', 'max_per_opoint')", false)
                        ],
                        'LM':[
                                max: queryToArray(sqlconn, "select * from dashboard.selectLastData('AQ', '-1 month', 'max')", false),
                                avg_per_magnitude: queryToArray(sqlconn, "select * from dashboard.selectLastData('AQ', '-1 month', 'avg_per_magnitude')", false),
                                min_per_magnitude: queryToArray(sqlconn, "select * from dashboard.selectLastData('AQ', '-1 month', 'min_per_magnitude')", false),
                                max_per_magnitude: queryToArray(sqlconn, "select * from dashboard.selectLastData('AQ', '-1 month', 'max_per_magnitude')", false),
                                max_per_opoint: queryToArray(sqlconn, "select * from dashboard.selectLastData('AQ', '-1 month', 'max_per_opoint')", false)
                        ]
                ],
                'MET': [
                        'L24H':[
                                avg_per_magnitude: queryToArray(sqlconn, "select * from dashboard.selectLastData('MET', '-24 hours', 'avg_per_magnitude')", true),
                                min_per_magnitude: queryToArray(sqlconn, "select * from dashboard.selectLastData('MET', '-24 hours', 'min_per_magnitude')", true),
                                max_per_magnitude: queryToArray(sqlconn, "select * from dashboard.selectLastData('MET', '-24 hours', 'max_per_magnitude')", true),
                                max_per_opoint: queryToArray(sqlconn, "select * from dashboard.selectLastData('MET', '-24 hours', 'max_per_opoint')", true)
                        ],
                        'L7D':[
                                avg_per_magnitude: queryToArray(sqlconn, "select * from dashboard.selectLastData('MET', '-7 days', 'avg_per_magnitude')", false),
                                min_per_magnitude: queryToArray(sqlconn, "select * from dashboard.selectLastData('MET', '-7 days', 'min_per_magnitude')", false),
                                max_per_magnitude: queryToArray(sqlconn, "select * from dashboard.selectLastData('MET', '-7 days', 'max_per_magnitude')", false),
                                max_per_opoint: queryToArray(sqlconn, "select * from dashboard.selectLastData('MET', '-7 days', 'max_per_opoint')", false)
                        ],
                        'LM':[
                                avg_per_magnitude: queryToArray(sqlconn, "select * from dashboard.selectLastData('MET', '-1 month', 'avg_per_magnitude')", false),
                                min_per_magnitude: queryToArray(sqlconn, "select * from dashboard.selectLastData('MET', '-1 month', 'min_per_magnitude')", false),
                                max_per_magnitude: queryToArray(sqlconn, "select * from dashboard.selectLastData('MET', '-1 month', 'max_per_magnitude')", false),
                                max_per_opoint: queryToArray(sqlconn, "select * from dashboard.selectLastData('MET', '-1 month', 'max_per_opoint')", false)
                        ]
                ]
        ]
        render(status: 200, contentType: "application/json", text: JsonOutput.toJson(ds))
    }

    @Override
    void setConfiguration(Config co) {
        csvMimeType = co.getProperty('grails.mime.types.csv', String, 'text/csv')
        encoding = co.getProperty('grails.converters.encoding', String, 'UTF-8')
    }
}
