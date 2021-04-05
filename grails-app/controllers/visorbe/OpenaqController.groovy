package visorbe

import grails.converters.JSON
import groovy.json.JsonBuilder
import groovy.json.StreamingJsonBuilder
import groovy.sql.Sql

import java.time.LocalDateTime
import java.time.ZonedDateTime

import static groovy.sql.Sql.*
import java.sql.Timestamp

class OpenaqController {
	static scope = "prototype"

	String csvMimeType

	String encoding

	static responseFormats = ['json', 'html']

	def modelService

	def index() { }

	def qryOpoint(params) {
		if(params.location != null) {
			def myOpoint = modelService.findOpoint(params.location)
			new StringWriter().with { w ->
				def json = new StreamingJsonBuilder(w)
				json {
					abbr(myOpoint?.abbr)
					location(myOpoint?.location)
					city(myOpoint?.city)
					country(myOpoint?.country)
					sourceType(myOpoint?.sourceType)
					sourceName(myOpoint?.sourceName)
					mobile(myOpoint?.mobile)
					coordinates(myOpoint?.coordinates)
					srid(myOpoint?.srid)
				}

				render(status: 200, contentType: "application/json", text: w.toString())
			}
		} else {
			new StringWriter().with { w ->
				new StreamingJsonBuilder(w).call(modelService.opoints4OpenAQ.collect{op ->
					   [abbr: op['abbr'],
						location: op['location'],
						x: op['x'],
						y: op['y'],
						srtid: op['srid'],
						city: op['city'],
						country: op['country'],
						sourceType: op['sourceType'],
						sourceName: op['sourceName'],
						mobile: op['mobile'],
						coordinates: op.coordinates]
				})
				render(status: 200, contentType: "application/json", text: w.toString())
			}
		}
	}

	def qryAQMagnitudes() {
		new StringWriter().with { w ->
			new StreamingJsonBuilder(w).call(modelService.openAQMagnitudes)
			render(status: 200, contentType: "application/json", text: w.toString())
		}
	}

	def qryDataset() {
		def error = ""

		def magnitude
		if(params.magnitude != null) {
			magnitude = modelService.openAQMagnitudes.find({it.magnitude == params.magnitude && it.magnitude_id != null})
			error += magnitude?'':"Parameter [magnitude] must be one of ${modelService.openAQMagnitudes.collect{it.magnitude}}. You also have the option of not using the parameter [magnitude]."
		}

		def itvl = (params.itvl != null && modelService.allowedIntervals.contains(params.itvl))?params.itvl:null
		error += itvl?'':"Interval pamameter [itvl] must be one of ${modelService.allowedIntervals}. "

		def opoint = null
		if(params.location != null) {
			opoint = modelService.findOpoint(params.location)
			error += opoint?'':"Parameter [location] must be one of ${modelService.opoints4OpenAQ.collect{it.abbr}} or ${modelService.opoints4OpenAQ.collect{it.location}}. You also have the option of not using the parameter [magnitude]."
		}

		Integer year = null
		if(params.year != null) {
			try {
				year = Integer.valueOf(params.year)
			} catch(Exception e) {
				year = null
				error += 'Invalid param [year] '
			}
		} else error += "Pamameter [year] must be defined. "

		Integer month = null
		if(params.month != null) {
			try {
				month = Integer.valueOf(params.month)
			} catch(Exception e) {
				month = null
				error += 'Invalid param [month] '
			}
		} else error += "Pamameter [month] must be defined. "

		Integer dom = null
		if(params.dom != null) {
			try {
				dom = Integer.valueOf(params.dom)
			} catch(Exception e) {
				dom = null
				error += 'Invalid param [dom] (day of month).'
			}
		} else error += "Pamameter [dom] (day of month) must be defined. "

		Integer hour = null
		if(params.hour != null) {
			try {
				hour = Integer.valueOf(params.hour)
			} catch(Exception e) {
				hour = null
				error += 'Invalid param [hour].'
			}
		} else hour = 0
		String strSql = ""
		if(error == "") {
			try {
				LocalDateTime.of(year.intValue(), month.intValue(), dom.intValue(), hour.intValue(), 0)
			} catch(Exception e) {
				error = 'params [year, month, dom, hour] don\'t conform a valid reference date and time.'
			}
		}
		if(error == "" && !opoint && !magnitude) {
			error = '[magnitude] or [location] must be defined, or both.'
		}

		if(error == '') {
			def myId = 'LCH'
			strSql = "select  * from dashboard.api_dataseries_vw('${itvl}','per hour',${magnitude?.magnitude_id},${opoint?.id},${year},${month},${dom},0)"
			def where = ''
			if(!magnitude) {where += ' where magnitude_id in (1,3,6,8,10,14)'}
			if(!opoint) {where += (where=='')?' where opoint_id in (1,2,5,6,7,8,9,14)':' and opoint_id in (1,2,5,6,7,8,9,14)'}
			if(where != '') {
				strSql = strSql + where
			}
			def dataset = []
			modelService.conn().eachRow(strSql) { row ->
				def values = [1:row.value24, 3:row.value24, 6:row.value8, 8:row.value1 , 10:row.value1, 14:row.value1]
				def myopint = modelService.findOpoint(row.opoint_abbr)
				def myMagnitude = modelService.openAQMagnitudes.find {it.magnitude == params.magnitude}
				if(myMagnitude) {
					dataset << [
						localdatetime:row.localdatetime,
						utcdatetime:  row.utcdatetime,
						magnitude_id: row.magnitude_id,
						opoint_id:    row.opoint_id,
						opoint_abbr:  row.opoint_abbr,
						value:        myMagnitude.factor==null?values[row.magnitude_id]:values[row.magnitude_id]*myMagnitude.factor,
						unit:         myMagnitude.factor==null?myMagnitude.original_unit:myMagnitude.unit,
						opoint:       myopint,
						magnitude:    myMagnitude
					]
				}
			}
			def result = dataset.collect {[
				date:[local:it.localdatetime, utc:it.utcdatetime],
				parameter:it.magnitude?.magnitude,
				unit:it.unit,
				value: it.value,
				averagingPeriod:it.magnitude?.averagingPeriod,
				location:it.opoint?.location,
				city:it.opoint?.city,
				country:it.opoint?.country,
				coordinates:it.opoint?.coordinates,
				sourceType:it.opoint?.sourceType,
				sourceName:it.opoint?.sourceName,
				mobile:it.opoint?.mobile,
				attribution:[
					["name": "REMMAQ", "url":"http://www.quitoambiente.gob.ec/ambiente"],
					["name": "Secretaría del Ambiente"],
					["name": "Municipio del Distrito Metropolitano de Quito"]
				]
			]}
			render(status: 200, contentType: "application/json", text: result as JSON)
		} else {
			def message = error
			render(status: 400, contentType: "application/json", text: '{"message":"'+ message +'", "error":400}')
		}
	}
}
