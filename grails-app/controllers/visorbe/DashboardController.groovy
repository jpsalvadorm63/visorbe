package visorbe

import grails.core.support.GrailsConfigurationAware
import grails.config.Config
import groovy.json.JsonBuilder
import groovy.json.StreamingJsonBuilder
import groovy.sql.Sql
import java.sql.Timestamp
import grails.converters.JSON
import java.util.zip.ZipEntry
import java.util.zip.ZipOutputStream
import static org.springframework.http.HttpStatus.OK

class DashboardController implements GrailsConfigurationAware {
	static scope = "prototype"
	String csvMimeType
	String encoding
	static responseFormats = ['json', 'html']
	def dataSource
	def modelService

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
			'10mX':'The max ten minutes average in one hour is calcualted after partitioning one hour in 6 parts (ten minutes each), then getting the average for each partition, and selecting the highest',
			'1hAc':'The sum of every minute in one hour'
		][rowc]
	}

	static def getRowDescription(row, lang='en') {
		return [
			'per hour':'Each row shows hourly data, depending on original raw data, may calculate the average, maximun, minimun, sum and/or max 10 minutes average from the original raw data.',
			'per day':'Each hourly data row calcualtes agregated data based on hourly data per day',
			'per month':'Each hourly data row calcualtes agregated data based on hourly data per month',
			'per year':'Each hourly data row calcualtes agregated data based on hourly data year',
			'total':'Each hourly data row calcualtes agregated data based on hourly data'
		][row]
	}

	class DatasetVw {
		Long    opoint_id
		Integer year
		Integer month
		Integer dom
		Integer hour
		String  opoint
		String  opoint_abbr
		Double  value1
		Double  value1x
		Double  value1y
		Double  value2
		Integer	n1
		Double  value8h
		Double  value24h
		Double  value1min
		Double  value1med
		Double  value1max
		Double  value2min
		Double  value2med
		Double  value2max
		Double  value8min
		Double  value8med
		Double  value8max
		Double  value24min
		Double  value24med
		Double  value24max

		String toString() {return "opoint=${this.opoint_id}, value${this.value}"}
	}

	def qryDataseriesVw() {
		// http://localhost:9090/dshbrd/dataseries?itvl=48+hour&level=per+day&magnitude_id=81&opoint_id=3&year=2020&month=9&dom=20&hour=0
		def mgn = modelService.magnitudeById(Integer.valueOf(params.magnitude_id))

		String sql = """
			select
				datetime,
				extract(year from datetime)  pyear,
				extract(month from datetime) pmonth,
				extract(day from datetime)   pdom,
				extract(hour from datetime)  phour,
				opoint_id,
				opoint,
				opoint_abbr,
				magnitude,
				magnitude_id,
				magnitude_abbr,
				magnitude_unit,
				value1,
                value1x,
                value1y,
				n1,
				value8,
                value24,
				value1min,
                value1med,
                value1max,
				value2,
				value2min,
                value2med,
                value2max,
                value8min,
                value8med,
                value8max,
				value24min,
                value24med,
                value24max
			from 
				dashboard.api_dataseries_vw(
					'${params.itvl}',
					'${params.level}', 
					${params.magnitude_id},
					${params.opoint_id},
					${params.year},
					${params.month}, 
					${params.dom},
                    ${params.hour}
				)
		"""

		def recordSet = []

		Timestamp _date = null
		Long      _magnitude_id = mgn?.id
		String    _magnitude = mgn?.pname
		String    _magnitude_abbr = mgn?.abbreviation
		String    _magnitude_unit = mgn?.pname_nasa

		def sqlconn = new Sql(dataSource)
		sqlconn.eachRow(sql) { row ->
			if(_date == null && row.datetime!=null) _date = row.datetime

			def newData = new DatasetVw(
				year:row.pyear,
				month:row.pmonth,
				dom:row.pdom,
				hour:row.phour,
				opoint_id:row.opoint_id,
				opoint:row.opoint,
				opoint_abbr:row.opoint_abbr,
				value1:row.value1,
				value1x:row.value1x,
				value1y:row.value1y,
				n1:row.n1,
				value8h:row.value8,
				value24h:row.value24,
				value2:row.value2,
				value1min:row.value1min,
				value1med:row.value1med,
				value1max:row.value1max,
				value2min:row.value2min,
				value2med:row.value2med,
				value2max:row.value2max,
				value8min:row.value8min,
				value8med:row.value8med,
				value8max:row.value8max,
				value24min:row.value24min,
				value24med:row.value24med,
				value24max:row.value24max
			)
			recordSet << newData
		}

		new StringWriter().with { w ->
			def json = new StreamingJsonBuilder(w)
			json.message {
				year(params.year)
				month(params.month)
				dom(params.dom)
				timelap(params.itvl)
				data(params.level)
				date(_date)
				magnitude(_magnitude)
				magnitude_id(_magnitude_id)
				magnitude_abbr(_magnitude_abbr)
				magnitude_unit(_magnitude_unit)
				dataSet(recordSet, { DatasetVw data ->
					year        data.year
					month       data.month
					dom         data.dom
					hour        data.hour
					opoint_id   data.opoint_id
					opoint      data.opoint
					opoint_abbr data.opoint_abbr
					value1      data.value1
					value1x     data.value1x
					value1y     data.value1y
					n1          data.n1
					value8h     data.value8h
					value24h    data.value24h
					value1min   data.value1min
					value1med   data.value1med
					value1max   data.value1max
					value2      data.value2
					value2min   data.value2min
					value2med   data.value2med
					value2max   data.value2max
					value8min   data.value8min
					value8med   data.value8med
					value8max   data.value8max
					value24min  data.value24min
					value24med  data.value24med
					value24max  data.value24max
				})
			}

			render(status: 200, contentType: "application/json", text: w.toString())
		}
	}

	class DatasetFc {
		Long    opoint_id
		Integer year
		Integer month
		Integer dom
		Integer hour
		Double  fc1
		Double  fc1x
		Double  fc1y

		String toString() {return "opoint=${this.opoint_id}, value${this.value}"}
	}

	def qryDataseriesFc() {
		// http://localhost:9090/dshbrd/dataseriesfc?itvl=48+hour&level=per+day&magnitude_id=81&opoint_id=3&year=2020&month=9&dom=20&hour=0
		def mgn = modelService.magnitudeById(Integer.valueOf(params.magnitude_id))

		String sql = """
			select
				datetime,
				extract(year from datetime)  pyear,
				extract(month from datetime) pmonth,
				extract(day from datetime)   pdom,
				extract(hour from datetime)  phour,
				magnitude_id,
				opoint_id,
				fc1,
                fc1x,
                fc1y
			from 
				dashboard.api_dataseriesfc(
					'${params.itvl}', 
					'${params.level}', 
					${params.magnitude_id}, 
					${params.opoint_id}, 
					${params.year}, 
					${params.month}, 
					${params.dom},
                    ${params.hour}
				)
			"""
		def recordSet = []

		Timestamp _date = null
		Long      _magnitude_id = mgn?.id

		def sqlconn = new Sql(dataSource)
		sqlconn.eachRow(sql) { row ->
			_date = (row.datetime!=null)?row.datetime:_date
			def newData = new DatasetFc(
				opoint_id:row.opoint_id,
				year:row.pyear,
				month:row.pmonth,
				dom:row.pdom,
				hour:row.phour,
				fc1:row.fc1,
				fc1x:row.fc1x,
				fc1y:row.fc1y
			)
			recordSet << newData
		}

		new StringWriter().with { w ->
			def json = new StreamingJsonBuilder(w)
			json.message {
				year(params.year)
				month(params.month)
				dom(params.dom)
				timelap(params.itvl)
				data(params.level)
				date(_date)
				magnitude_id(_magnitude_id)
				dataSet(recordSet, { DatasetFc data ->
					year      data.year
					month     data.month
					dom       data.dom
					hour      data.hour
					opoint_id data.opoint_id
					fc1       data.fc1
					fc1x      data.fc1x
					fc1y      data.fc1y
				})
			}
			render(status: 200, contentType: "application/json", text: w.toString())
		}
	}

	def qryDistances() {
		// http://localhost:9090/dshbrd/distances
		int maxDistance = 0
		try {
			maxDistance = params.max?Integer.parseInt(params.max):2500
		} catch(e) {
			maxDistance = 2500
		}

		int zonetype = 0
		try {
			zonetype = params.zonetype?Integer.parseInt(params.zonetype):1
		} catch(e) {
			zonetype = 1
		}

		def mydistances = modelService.readDistances(maxDistance, zonetype)
		render(status: 200, contentType: "application/json", text: mydistances as JSON)
	}

	def qryDataseriesDn() {
		// service call sample
		// http://localhost:9090/dshbrd/dataseriesdn?itvl=24+hours&magnitudes=(6)&opoints=(3,2,6,1,5,7,9,4,14,8)&mode=station&year=2020&month=11&dom=16&timelap=7+day&presentations=(10,20,30,40,50,60)&lang=en

		// params
		def presentations = params.presentations[1..-2].split(',')
		String lang = params.lang?params.lang:'en'
		def magnitudes = params.magnitudes
		def opoints = params.opoints
		def itvl = params.itvl
		def year = params.year as Integer
		def month = params.month as Integer
		def dom = params.dom as Integer
		def date = new Date().copyWith(year: year, month: month - 1 , dayOfMonth: dom, hourOfDay: 0, minute: 0, second: 0)

		// file names
		String filename = "remmaqvisor_data_${new Date().format('yyyymmdd-HHmm')}"
		String filenamezip = "${filename}.zip"
		String filenamecsv = "${filename}.csv"
		String filenamemd = "${filename}.md"

		// variables
		def contador = 0

		// stream init
		def outs = response.outputStream
		response.status = OK.value()
		response.setContentType("APPLICATION/OCTET-STREAM")
		response.setHeader "Content-disposition", "attachment; filename=${filenamezip}"
		ZipOutputStream zip = new ZipOutputStream(outs)

		// mark down file producer
		zip.putNextEntry(new ZipEntry(filenamemd))
		zip.write("# REMMAQ|Visor System, Data Series Downloder Module\n\n".bytes)
		zip.write("## ${(lang == 'en')?'Params':'Par\u00E1metros'}\n\n".bytes)
		zip.write("\n${(lang == 'en')?'Lenguaje':'Language'}: ${lang}\n\n".bytes)
		zip.write("${(lang == 'en')?'Data series from:':'Series de datos desde:'} ${date?.format('dd/MMM/yyyy')}, ${itvl}\n\n".bytes)
		zip.write("${(lang == 'en')?'Magnitudes:':'Magnitudes:'}\n\n".bytes)
		for(magnitude in magnitudes[1..-2].split(',')) {
			if(magnitude?.isInteger()) {
				def schama = modelService.getMagnitudeSchema(magnitude as int)
				zip.write("  *  ${schama?.magnitude_name?.getAt(lang)}\n".bytes)
			}
		}
		zip.write("\n${(lang == 'en')?'Stations:':'Estaciones:'}\n\n".bytes)
		for(opoint in opoints[1..-2].split(',')) {
			if(opoint?.isInteger()) {
				def schema = getOpoint(opoint as int)
				zip.write("  *  ${schema?.pname}\n".bytes)
			}
		}
		zip.write("\n## ${(lang == 'en')?'Column description':'Descriptor de columnas'}\n\n".bytes)
		zip.write("* ${(lang == 'en')?'**magnitude**':'**magnitud**'}: ${(lang == 'en')?'Magnitude name':'Nombre de la magnitud'}\n\n".bytes)
		zip.write("* ${(lang == 'en')?'**station**':'**estaci\u00F3n**'}: ${(lang == 'en')?'Station name':'Nombre de la estaci\u00F3n'}\n\n".bytes)
		zip.write("* ${(lang == 'en')?'**localdatetime**':'**fecha hora local**'}: ${(lang == 'en')?'Survey date and time at Quito':'Fecha y hora de la toma del dato en Quito '}\n\n".bytes)
		zip.write("* ${(lang == 'en')?'**utcdatetime**':'**fecha hora UTC**'}: ${(lang == 'en')?'UTC date and time':'Fecha y hora UTC'}\n\n".bytes)
		for(presentation in presentations) {
			if(presentation == '10') {
				if(lang == 'en') {
					zip.write("* **data**: Surveyed data'}\n".bytes)
					zip.write("* **data units**: units of surveyed data\n".bytes)
					zip.write("* **type**: Data type according to the next list'}\n".bytes)
				} else {
					zip.write("* **data**: Dato levantado\n".bytes)
					zip.write("* **unidades**: unidad de cada dato según magnitud\n".bytes)
					zip.write("* **tipo**: Tipo de dato segun la siguiente lista:\n".bytes)
				}

				[['1hC',  '1 hour average concentration', 'Concentraci\u00F2n promedio 1 hora'],
				 ['8hC',  '8 hours average concentration', 'Concentraci\u00F2n promedio \u00FAltimas 8 horas'],
				 ['24hC', 'last 24 hours average concentration', 'Concentraci\u00F3n promedio \u00FAltimas 24 horas'],
				 ['10mX', 'Max 10 minutes average', 'M\u00E1ximo promedio de10 minuntes'],
				 ['1hC',  '1 hour total sum', 'Total acumulado en 1 hora'],
				 ['1hP',  '1 hour average', 'promedio en 1 hora']].each {
					zip.write("  * **${it[0]}**: ${(lang == 'en')?it[1]:it[2]}\n".bytes)
				}
			}
			if(presentation == '20') {
				zip.write("* **min**: ${(lang == 'en')?'Min data':'M\u00EDnimo'}\n".bytes)
				zip.write("* **med**: ${(lang == 'en')?'Median':'Mediana (quintil 0.5)'}\n".bytes)
				zip.write("* **max**: ${(lang == 'en')?'Max data':'M\u00E1ximo'}\n".bytes)
			}
			if(presentation == '40') {
				zip.write("* **IQCA**: ${(lang == 'en')?'Quito City Air Quality Index':'Indice de calidad del Aire de la ciudad de Quito'}\n".bytes)
			}
			if(presentation == '50') {
				zip.write("* **AQI**: ${(lang == 'en')?'Aire Quality International Index':'Indice internacional de calidad del Aire'}\n".bytes)
			}
			if(presentation == '60') {
				zip.write("* ${(lang == 'en') ? '**another data**' : '**otro dato**'}: ${(lang == 'en')?'The value of another concentration':'El dato de otra concentraci\u00F2n'}\n".bytes)
				zip.write("* ${(lang == 'en') ? '**another data type**' : '**tipo otro dato**'}: ${(lang == 'en')?'Tipe of another type':'Tipo de la otra concentraci\u00F2n'}\n".bytes)
			}
		}
		zip.flush()
		zip.closeEntry()

		// database init
		String mysql = """
			select
				magnitude_id,
				opoint_id,
				datetime localdatetime,
				magnitude_unit,
				utcdatetime,
				value1,
				value1min,
				value1med,
				value1max,
				value8 value8h,
				value8min,
				value8med,
				value8max,
				value24 value24h,
				value24min,
				value24med,
				value24max
			from
				dashboard.api_dataseries_vw(
					'${itvl}',
					'per hour',
					null,
					null,
					${year},
					${month},
					${dom},
                    0
				)
			where magnitude_id in ${magnitudes} and opoint_id in ${opoints}
            order by 1, 2, 3
		"""
		def sqlconn = new Sql(dataSource)

		// header init
		def header = (lang == 'en') ? ['magnitude','station','localdatetime','utcdatetime'] : ['magnitud','estaci\u00F3n','fecha hora local','fecha hora UTC']
		for(presentation in presentations) {
			if(presentation == '10') {
				header.push('data');
				header.push((lang == 'en')?'data units':'unidades')
				header.push((lang == 'en')?'type':'tipo')
			}
			if(presentation == '20') {
				header.push('min')
				header.push('med')
				header.push('max')
			}
			if(presentation == '40') {
				header.push('IQCA')
			}
			if(presentation == '50') {
				header.push('AQI')
			}
			if(presentation == '60') {
				header.push((lang == 'en')?'another data':'otro dato')
				header.push((lang == 'en')?'another data type':'tipo otro dato')
			}
		}

		// data series: csv file producer
		zip.putNextEntry(new ZipEntry(filenamecsv))
		zip.write("${header.join(';')}\n".bytes)
		sqlconn.eachRow(mysql) { rr ->
			def row = rr.toRowResult()
			row.isHourlyData = true
			// magnitude
			def magnitudeName = getMagnitudeSchema(row?.magnitude_id)?.magnitude_name[lang]
			def opointName = getOpoint(row?.opoint_id)?.pname
			def schema = getMagnitudeSchema(row?.magnitude_id)
			def line = [magnitudeName, opointName, row?.localdatetime, row?.utcdatetime]
			for(presentation in presentations) {
				if(presentation == '10') {
					def res = schema?.DATA?.value(row)
					line.push(res?res:'')
					line.push((schema?.unit)? schema?.unit : '')
					line.push((schema?.DATA?.colDescription)?schema?.DATA?.colDescription?.call(lang)?.getAt(0):'')
				}
				if(presentation == '20') {
					def resmin = schema?.DATA?.valuemin?.call(row)
					line.push(resmin?resmin:'')
					def resmed = schema?.DATA?.valuemed?.call(row)
					line.push(resmed?resmed:'')
					def resmax = schema?.DATA?.valuemax?.call(row)
					line.push(resmax?resmax:'')
				}
				if(presentation == '40') {
					def res = schema?.IQCA?.value?.call(row)
					line.push(res?res: '')
				}
				if(presentation == '50') {
					def res = schema?.AQI?.value?.call(row)
					line.push(res?res: '')
				}
				if(presentation == '60') {
					def res = schema?.CONCENTRATION?.value?.call(row)
					line.push(res?res:'')
					line.push((schema?.CONCENTRATION?.colDescription)?schema?.CONCENTRATION?.colDescription?.call(lang)?.getAt(0):'')
				}
			}
			zip.write("${line.join(';')}\n".bytes)
			contador++
			if(contador % 192 == 0) {
				zip.flush()
				contador = 0
			}
		}
		zip.flush()
		zip.closeEntry()

		// close zip file
		zip.close()
	}

	class DashboardRec {
		Timestamp datetime
		Long      magnitude_id
		Integer   opoint_id
		Float     value1min
		Float     value1
		Float     value1x
		Float     value1y
		Float     value1max
		Float     value8min
		Float     value8h
		Float     value8max
		Float     value24min
		Float     value24h
		Float     value24max
		Float     value2
		Integer   n1
		Boolean   isHourlyData
	}

	def qryDataseriesDashboard() {
		// service call sample
		// http://localhost:9090/dshbrd/dashboard?magn=1&itvl=-24+hours
		// http://localhost:9090/dshbrd/dashboard?magn=1&year=2019
		// http://localhost:9090/dshbrd/dashboard?magn=1&year=2019&month=7
		Integer magn = params['magn']?(params['magn'] as Integer):0
		if(magn == 81) magn = 82
		Integer year = params['year']?(params['year'] as Integer):null
		Integer month = params['month']?((params['month'] as Integer) + 1):null
		String itvl  = params['itvl']?params['itvl']:null
		def magnitudeSchema = modelService.getMagnitudeSchema(magn)
		def isHourlyData = (itvl == '-24 hours')
		def maxData = null
		def avgDATA = []
		def avgDATAX = []
		def avgDATAY = []
		def avgIQCA = []
		def avgAQI  = []
		def opoints = []
		if(magnitudeSchema != null) {
			def recordSet = []
			def sqlconn = new Sql(dataSource)
			def sqlstr = """
				select
					 mdatetime      datetime,
					 mmagnitude_id  magnitude_id,
					 mopoint_id     opoint_id,
					 mvalue1min     value1min,
					 mvalue1        value1,
					 mvalue1x       value1x,
					 mvalue1y       value1y,
					 mvalue1max     value1max,
					 mvalue8min     value8min,
					 mvalue8        value8h,
					 mvalue8max     value8max,
					 mvalue24min    value24min,
					 mvalue24       value24h,
					 mvalue24max    value24max,
					 mvalue2        value2,
					 mn1            n1
				from dashboard.api_dashboard_itvl_vw(${magn}, ${year}, ${month}, ${itvl})
				order by 3,2,1
			"""

			sqlconn.eachRow(sqlstr) {row ->
				def newData           = new DashboardRec()
				newData.datetime      = row.datetime
				newData.magnitude_id  = row.magnitude_id
				newData.opoint_id     = row.opoint_id
				newData.value1min     = row.value1min
				newData.value1x       = row.value1x
				newData.value1y       = row.value1y
				newData.value1        = (magn != 81 && magn != 82)?row.value1:(Math.sqrt(row.value1x*row.value1x + row.value1y*row.value1y) as Float)
				newData.value1max     = row.value1max
				newData.value8min     = row.value8min
				newData.value8h       = row.value8h
				newData.value8max     = row.value8max
				newData.value24min    = row.value24min
				newData.value24h      = row.value24h
				newData.value24max    = row.value24max
				newData.value2        = row.value2
				newData.n1			  =	row.n1
				newData.isHourlyData  = false

				if(newData.datetime != null && newData.magnitude_id != null && newData.opoint_id != null) {
					def recData = [
						opoint_id: newData.opoint_id,
						datetime: newData.datetime,
						DATA: magnitudeSchema.DATA?.value?.call(newData),
						DATAX: magnitudeSchema.DATA?.valuex?.call(newData),
						DATAY: magnitudeSchema.DATA?.valuey?.call(newData),
						CONCENTRATION: magnitudeSchema.CONCENTRATION?.value?.call(newData),
						IQCA: magnitudeSchema.IQCA?.value?.call(newData),
						AQI: magnitudeSchema.AQI?.value?.call(newData)
					]
					recordSet << recData
					if(recData.DATA != null) avgDATA << recData.DATA
					if(recData.DATAX != null) avgDATAX << recData.DATAX
					if(recData.DATAY != null) avgDATAY << recData.DATAY
					if(recData.IQCA != null) avgIQCA << recData.IQCA
					if(recData.AQI != null) avgAQI << recData.AQI
					// set max data for the city
					maxData = (maxData == null)?recData:((recData.DATA > maxData.DATA)?recData:maxData)
					// set opoints
					if(!opoints.contains(newData.opoint_id)) opoints << newData.opoint_id
				}
			}

			new StringWriter().with { w ->
				def json = new StreamingJsonBuilder(w)
				json {
					report('REMMAQ - VISOR, **DASHBOARD REPORT**')
					magnitude_id(magn)
					if(itvl != null) "interval"(itvl)
					if(year != null) "myear"(year)
					if(month != null) "mmonth"(month)
					worst {
						opoint_id(maxData?.opoint_id)
						datetime(maxData?.datetime)
						if(maxData?.DATA != null) {
							DATA(maxData?.DATA)
							DATAX(maxData?.DATAX)
							DATAY(maxData?.DATAY)
							IQCA(maxData?.IQCA)
							AQI(maxData?.AQI)
						} else {
							DATA(null)
							IQCA(null)
							AQI(null)
						}
					}
					avg {
						if(maxData?.DATA != null && (avgDATA.size() > 0)) {
							DATA(avgDATA.sum()/(avgDATA.size() as float))
							if(magnitudeSchema.IQCA != null && avgIQCA?.size() > 0) IQCA(avgIQCA.sum()/(avgIQCA.size() as float))
							if(magnitudeSchema.AQI != null && avgAQI?.size() > 0) AQI(avgAQI.sum()/(avgAQI.size() as float))
							if(avgDATAX.size() > 0) {
								DATAX(avgDATAX.sum()/(avgDATAX.size() as float))
								DATAY(avgDATAY.sum()/(avgDATAY.size() as float))
							} else {
								DATAX(null)
								DATAY(null)
							}
						} else {
							DATA(null)
							DATAX(null)
							DATAY(null)
							IQCA(null)
							AQI(null)
						}
					}
					dataSeries {
						opoints.each { op ->
							def subset = recordSet.findAll {data -> data?.opoint_id == op}
							def maxDATA = subset.max {it?.DATA}
							def sumDATA = subset.sum {it?.DATA}
							def sumIQCA = subset.sum {it?.IQCA}
							def sizeDATA = subset.size()
							def minDATA = subset.min {it?.DATA}
							"${op}"  {
								DATA {
									max maxDATA.DATA
									maxdatetime maxDATA.datetime
									sum sumDATA
									avg ((sizeDATA > 0)? sumDATA/sizeDATA : null)
									min minDATA.DATA
									mindatetime minDATA.datetime
								}
								if(magnitudeSchema.IQCA != null) {
									IQCA {
										max maxDATA.IQCA
										maxdatetime maxDATA.datetime
										avg ((sizeDATA > 0)?sumIQCA/sizeDATA : null)
										min minDATA.IQCA
										mindatetime minDATA.datetime
									}
								}
								if(magnitudeSchema.AQI != null) {
									AQI {
										max maxDATA.AQI
										maxdatetime maxDATA.datetime
										min minDATA.AQI
										mindatetime minDATA.datetime
									}
								}
//								ds(subset.collect {[datetime: it?.datetime, DATA: it?.DATA, IQCA: it?.IQCA, AQI: it?.AQI]})
							}
						}
					}
				}
				render(status: 200, contentType: "application/json", text: w.toString())
			}
		} else {
			render(status: 200, contentType: "application/json", text: '"no":"data"')
		}
	}

	@Override
	void setConfiguration(Config co) {
		csvMimeType = co.getProperty('grails.mime.types.csv', String, 'text/csv')
		encoding = co.getProperty('grails.converters.encoding', String, 'UTF-8')
	}
}
