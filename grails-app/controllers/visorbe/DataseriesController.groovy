package visorbe

import grails.config.Config
import grails.core.support.GrailsConfigurationAware
import grails.rest.*
import grails.converters.*
import groovy.sql.Sql

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
            '24hC':'Each hourly concentration data is calculated as the average of the previous 24 hours. For exmple: the average at 12H00 is the average between 12H00 (previous day) and 11H59',
            '8hC':'Each hourly concentration data is calculated as the average of the previous 8 hours. For exmple: the average at 12H00 is the average between 04H00 and 11H59',
            '1hC':'Each hourly concentration data is calculated as the average of the next 60 minutes. For example: the average at 12H00 is the average between 12H00 and 12H59',
            '1hP':'The one hour average is computed as the average of the next 60 minutes. For example: the average at 12H00 is the average between 12H00 and 12H59',
            '10mX':'The max ten minutes average in one hour is calculated after partitioning one hour in 6 parts (ten minutes each), then getting the average for each partition, and selecting the highest',
            '1hAc':'The sum of every minute in one hour'
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

    /**
     *
     * @return
     */
    def qryDsCSV() {
        // service call sample
        // http://www.aqvisor.net:9090/visorbe-0.2/dshbrd/csvdn?lang=en&magnitude=14&opoint=null&year=2021&month=3&dom=25&hour=0&itvl=1+hour&mode=remmaq&row=per+hour&show=data&complete=true&decimalp=false

        def show = params.show

        def outs = response.outputStream
        response.status = OK.value()
        response.contentType = "&#36;{csvMimeType};charset=&#36;{encoding}"
        if(show == 'help') {
            response.setHeader "Content-disposition", "attachment; filename=REMMAQ_HELP_dshbrd_csvdn.md"
            outs << "# Endpoint: http:/www.aqvisor.net:9090/dshbrd/csvdn?...(params)\n".bytes
            outs << "## Params:\n".bytes
            outs << "* **lang** .- the language of reports: **en** for english, **es** for spanish. Fur example: `http://www.aqvisor.net:9090/dshbrd/csvdn?lang=en`\n".bytes
            outs << "* **magnitde** .- magnitude code, accordiong the next list:\n".bytes
            for(def mg in modelService.magnitude_schemas) {
                outs << "	* `${mg.magnitude_id}` = `${mg.magnitude_name['en']}, spanish: `${mg.magnitude_name['es']}`\n"
            }
            outs << "* **opoint** .- station code, accordiong the next list:\n".bytes
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
            outs << "* **sample**\n".bytes
            outs << "`http://www.aqvisor.net:9090/visorbe-0.2/dshbrd/csvdn?lang=en&magnitude=14&opoint=null&year=2021&month=3&dom=25&hour=0&itvl=24+hours&mode=remmaq&row=per+hour&show=data&complete=true&decimalp=false`\n"
        }
        else
        {
            def getMagnitudeSchema = modelService.getMagnitudeSchema

            // params
            String lang = params.lang?params.lang:'en'
            def magnitude_id = params.magnitude as Integer
            def magnitude_schema = getMagnitudeSchema(magnitude_id)
            def opoint_id = (!params.opoint || params.opoint == 'null')?null:(params.opoint as Integer)
            def opoint = (opoint_id != null)?modelService.getOpoint2(opoint_id):null
            def year = params.year?(params.year as Integer):null
            def month = params.month?(params.month as Integer):null
            def dom = params.dom?(params.dom as Integer):null
            def hour = params.dom?(params.hour as Integer):0
            def itvl = params.itvl?params.itvl:null
            def row = params.row?:'per hour'
            def rowDesc = [
                'per hour':(lang == 'en')?'HOURLY':'HORARIO',
                'per day':(lang == 'en')?'DAYLY':'DIARIO',
                'per month':(lang == 'en')?'MONTHLY':'MENSUAL',
                'per year':(lang == 'en')?'ANNUAL':'ANUAL',
                'total':'TOTAL'
            ][row]
            def mode = (params.mode == 'nasa')?'nasa':'remmaq'
            def decimalp = (params.decimalp == 'true')?'.':','
            def complete =  (params.complete == 'true')

            //output file names
            def date = createDate(year, month, dom, hour)
            String filename = "REMMAQ_${date?date.format('yyyyMMdd-HHmm'):'ERROR'}"

            String filenamemd = "${filename}.md"
            String filenamecsv = "${filename}.csv"

            // input params erors
            def errors = ""
            if(magnitude_id == null) errors = errors + '  * Magnitude param is not valid (magnitude param)\n'
            if(magnitude_schema == null) errors = errors + '  * Unknown magnitude code (magnitude param)\n'
            if(date == null) errors = errors + '  * wrong date parameters (year, month, dom, hour params)\n'
            if(itvl == null) errors = errors + '  * interval must be defined (`itvl` param)\n'
            if(opoint_id != null && opoint == null) errors = errors + '  * unknown station code (`opoint` param)\n'
            if(!['per hour', 'per day', 'per month', 'per year', 'total'].contains(row)) errors = errors + '  * unknown row code (`row` param)\n'

            if(show == 'metadata') {
                response.setHeader "Content-disposition", "attachment; filename=${filenamemd}"
                if(errors.length() > 0) {
                    outs << "# REMMAQ|Visor System, Dataseries Downloader\n\n".bytes
                    outs << "## Params\n\n".bytes
                    outs << "### **Errors**\n\n".bytes
                    outs << "${errors}\n\n".bytes
                } else {
                    outs << "# **`${magnitude_schema.magnitude_name['en']}`**\n\n".bytes
                    outs << "REMMAQ|Visor System, Dataseries Downloader\n\n".bytes
                    outs << "## Params\n\n".bytes
                    outs << "  * Date and interval: from ${date.format('yyyy.MM.dd-HH:mm')}, ${itvl}. ${(lang == 'en')?'Each data row is ':'Each fila representa un dato '} ${rowDesc} ${(lang == 'en')?' in the interval':' En el intervalo '}\n\n".bytes
                    outs << "  * Magnitude: ${magnitude_schema.magnitude_name[lang]} \n\n".bytes
                    if(magnitude_schema.nasa_name != null) {
                        outs << "  * Nasa name: `${magnitude_schema.nasa_name}` \n\n".bytes
                    }
                    outs << "  * Station: `${(opoint_id != null)?opoint:'all'}` \n\n".bytes
                    outs << "\n### *Magnitude details*\n\n".bytes
                    outs << "  * Magnitude name in english: **`${magnitude_schema.magnitude_name['en']}`**\n\n".bytes
                    outs << "  * Magnitude name in spanish: **`${magnitude_schema.magnitude_name['es']}`**\n\n".bytes
                    outs << "  * NASA magnitude name: **`${magnitude_schema.nasa_name?magnitude_schema.nasa_name:'n/a'}`**\n\n".bytes
                    outs << "  * Units: **`${magnitude_schema.unit?magnitude_schema.unit:'n/a'}`**\n\n".bytes
                    outs << "  * Base data: **${magnitude_schema.DATA.colDescription('en')[0]}**, ${magnitude_schema.DATA.colDescription('en')[1]}, ${getDataDescription(magnitude_schema.DATA.colDescription('en')[0])}\n\n".bytes
                    outs << "  * Row width: **${row}**, ${getRowDescription(row)}\n\n".bytes

                    outs << "## Columns\n\n".bytes
                    outs << "### Magnituide and date time Columns\n\n".bytes
                    outs << "  * **magnitude** .- magnitude name\n\n".bytes
                    outs << "  * **station** .- station name\n\n".bytes
                    outs << "  * **datetime** .- local date and time\n\n".bytes
                    outs << "  * **utcdatetime** .- equivalent UTC date and time\n\n".bytes
                    outs << "### Hourly based ${row!='per hour'?(', ' + rowDesc + ' aggregated '):''} Columns\n\n".bytes
                    outs << "  * **1hC** .- ${row!='per hour'?(row + ' aggregated based on '):''}hourly data${row!='per hour'?(' groupped on Magnitude/station/datetime '):''}\n\n".bytes
                    if(complete) {
                        outs << "  * **1hC_x** .- x axis data component\n\n".bytes
                        outs << "  * **1hC_x** .- y axis data component\n\n".bytes
                        outs << "  * **1hC_min** .- minimum\n\n".bytes
                        outs << "  * **1hC_max** .- maximum\n\n".bytes
                    }
                    outs << "### Hourly forecasting data based ${row!='per hour'?(', ' + rowDesc + ' aggregated '):''} Columns\n\n".bytes
                    outs << "  * **fc_1hC** .- forecasting ${row!='per hour'?(row + ' aggregated based on '):''}hourly data${row!='per hour'?(' groupped on Magnitude/station/datetime '):''}\n\n".bytes
                    if(complete) {
                        outs << "  * **fc_1hC_x** .- forecasting x axis data component\n\n".bytes
                        outs << "  * **fc_1hC_x** .- forecasting y axis data component\n\n".bytes
                    }
                    outs << "### 8 hours moving average data ${row!='per hour'?(', ' + rowDesc + ' aggregated '):''} Columns\n\n".bytes
                    outs << "  * **8hC** .- ${row!='per hour'?(row + ' aggregated based on '):''}hourly data${row!='per hour'?(' groupped on Magnitude/station/datetime '):''}\n\n".bytes
                    if(complete) {
                        outs << "  * **8hC_min** .- minimum\n\n".bytes
                        outs << "  * **8hC_max** .- maximum\n\n".bytes
                    }
                    outs << "### 8 forecasting hours moving average data ${row!='per hour'?(', ' + rowDesc + ' aggregated '):''} Columns\n\n".bytes
                    outs << "  * **fc_8hC** .- ${row!='per hour'?(row + ' aggregated based on '):''}hourly data${row!='per hour'?(' groupped on Magnitude/station/datetime '):''}\n\n".bytes
                    if(complete) {
                        outs << "  * **fc_8hC_min** .- minimum\n\n".bytes
                        outs << "  * **fc_8hC_max** .- maximum\n\n".bytes
                    }
                    outs << "### 24 hours moving average data ${row!='per hour'?(', ' + rowDesc + ' aggregated '):''} Columns\n\n".bytes
                    outs << "  * **24hC** .- ${row!='per hour'?(row + ' aggregated based on '):''}hourly data${row!='per hour'?(' groupped on Magnitude/station/datetime '):''}\n\n".bytes
                    if(complete) {
                        outs << "  * **24hC_min** .- minimum\n\n".bytes
                        outs << "  * **24hC_max** .- maximum\n\n".bytes
                    }
                    outs << "### 24 forecasting hours moving average data ${row!='per hour'?(', ' + rowDesc + ' aggregated '):''} Columns\n\n".bytes
                    outs << "  * **fc_24hC** .- ${row!='per hour'?(row + ' aggregated based on '):''}hourly data${row!='per hour'?(' groupped on Magnitude/station/datetime '):''}\n\n".bytes
                    if(complete) {
                        outs << "  * **fc_24hC_min** .- minimum\n\n".bytes
                        outs << "  * **fc_24hC_max** .- maximum\n\n".bytes
                    }
                    outs << "### Indexces \n\n".bytes
                    outs << "  * **IQCA** .- Quito Air Quality Index\n\n".bytes
                    if(complete) {
                        outs << "  * **fc_IQCA** .- Quito Air Quality forecasting Index\n\n".bytes
                    }
                    outs << "  * **AQI** .- Air quality international (USA) index\n\n".bytes
                    if(complete) {
                        outs << "  * **fc_AQI** .- Air quality international (USA) forecasting index\n\n".bytes
                    }
                }
            }
            else
            {
                response.setHeader "Content-disposition", "attachment; filename=${filenamecsv}"
                def magnitude_schema2 = getMagnitudeSchema(magnitude_id==81?82:magnitude_id==82?81:null)
                if(complete) {
                    outs << "magnitude;station;datetime;utcdatetime;1hC;1hC_x;1hC_y;1hC_min;1hC_max;fc_1hC;fc_1hC_x;fc_1hC_y;8hC;8hC_min;8hC_max;fc_8hC_max;fc_8hC_min;fc_8hC_max;24hC;24hC_min;24hC_max;fc_24hC;fc_24hC_min;fc_24hC_max;IQCA;fc_IQCA;AQI;fc_AQI\n".bytes
                } else {
                    outs << "magnitude;station;datetime;utcdatetime;1hC;8hC;24hC;IQCA;AQI\n"
                }

                String mysql = modelService.getSql4_1mg (itvl, row, magnitude_id, opoint_id, year, month, dom, hour)

                def sqlconn = new Sql(dataSource)
                int n = 0
                sqlconn.eachRow(mysql) { rr ->
                    def rrfc = [
                        isHourlyData:rr?.isHourlyData,
                        value1:rr?.fc1,
                        value1max:rr?.fc1max,
                        value8h:rr?.fc8,
                        value8hmax:rr?.fc8max,
                        value24h:rr?.fc24,
                        value24hmax:rr?.fc24max
                    ]
                    outs << "${(mode=='nasa' && magnitude_schema?.nasa_name)?magnitude_schema.nasa_name:magnitude_schema?.magnitude_name?.getAt(lang)};".bytes
                    outs << "${modelService.getOpoint2(rr?.opoint_id)?.opoint_name};".bytes
                    outs << "${rr?.datetime};".split(' ').join('T').bytes
                    outs << "${rr?.utcdatetime};".bytes
                    outs << "${rr?.value1?Math.round(rr?.value1*10.0)/10.0:''};".replace('.',decimalp).bytes
                    if(complete) {
                        outs << "${rr?.value1x?Math.round(rr?.value1x*10.0)/10.0:''};".replace('.',decimalp).bytes
                        outs << "${rr?.value1y?Math.round(rr?.value1y*10.0)/10.0:''};".replace('.',decimalp).bytes
                        outs << "${rr?.value1min ?Math.round(rr?.value1min*10.0)/10.0: ''};".replace('.', decimalp).bytes
                        outs << "${rr?.value1max ?Math.round(rr?.value1max*10.0)/10.0: ''};".replace('.', decimalp).bytes
                        outs << "${rr?.fc1 ?Math.round(rr?.fc1*10.0)/10.0: ''};".replace('.', decimalp).bytes
                        outs << "${rr?.fc1x ?Math.round(rr?.fc1x*10.0)/10.0: ''};".replace('.', decimalp).bytes
                        outs << "${rr?.fc1y ?Math.round(rr?.fc1y*10.0)/10.0: ''};".replace('.', decimalp).bytes
                    }
                    outs << "${rr?.value8h?Math.round(rr?.value8h*10.0)/10.0:''};".replace('.',decimalp).bytes
                    if(complete) {
                        outs << "${rr?.value8min?Math.round(rr?.value8min*10.0)/10.0: ''};".replace('.', decimalp).bytes
                        outs << "${rr?.value8max?Math.round(rr?.value8max*10.0)/10.0: ''};".replace('.', decimalp).bytes
                        outs << "${rr?.fc8?Math.round(rr?.fc8*10.0)/10.0: ''};".replace('.', decimalp).bytes
                        outs << "${rr?.fc8min?Math.round(rr?.fc8min*10.0)/10.0: ''};".replace('.', decimalp).bytes
                        outs << "${rr?.fc8max?Math.round(rr?.fc8max*10.0)/10.0: ''};".replace('.', decimalp).bytes
                    }
                    outs << "${rr?.value24h?Math.round(rr?.value24h*10.0)/10.0:''};".replace('.',decimalp).bytes
                    if(complete) {
                        outs << "${rr?.value24min?Math.round(rr?.value24min*10.0)/10.0: ''};".replace('.', decimalp).bytes
                        outs << "${rr?.value24max?Math.round(rr?.value24max*10.0)/10.0: ''};".replace('.', decimalp).bytes
                        outs << "${rr?.fc24?Math.round(rr?.fc24*10.0)/10.0: ''};".replace('.', decimalp).bytes
                        outs << "${rr?.fc24min?Math.round(rr?.fc24min*10.0)/10.0: ''};".replace('.', decimalp).bytes
                        outs << "${rr?.fc24max?Math.round(rr?.fc24max*10.0)/10.0: ''};".replace('.', decimalp).bytes
                    }
                    def iqca = magnitude_schema?.IQCA?.value?.call(rr)
                    outs << "${iqca?iqca:''};".replace('.',decimalp).bytes
                    if(complete) {
                        def fc_iqca = magnitude_schema?.IQCA?.value?.call(rrfc)
                        outs << "${fc_iqca?Math.round(fc_iqca):''};".replace('.', decimalp).bytes
                    }
                    def aqi = magnitude_schema?.AQI?.value?.call(rr)
                    outs << "${aqi?aqi:''};".replace('.',decimalp).bytes
                    if(complete) {
                        def fc_aqi = magnitude_schema?.AQI?.value?.call(rrfc)
                        outs << "${fc_aqi?fc_aqi: ''}".replace('.', decimalp).bytes
                    }
                    outs << "\n"
                    n++
                    if(n==1024) {
                        outs.flush()
                        n=0
                    }
                }
            }
        }
        outs.flush()
        outs.close()
    }

    @Override
    void setConfiguration(Config co) {
        csvMimeType = co.getProperty('grails.mime.types.csv', String, 'text/csv')
        encoding = co.getProperty('grails.converters.encoding', String, 'UTF-8')
    }
}
