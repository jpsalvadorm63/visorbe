package visorbe

import grails.config.Config
import grails.core.support.GrailsConfigurationAware
import groovy.sql.Sql

import java.sql.Array

import static org.springframework.http.HttpStatus.OK


class WsController implements GrailsConfigurationAware {
	static scope = "prototype"

	String csvMimeType
	String encoding
	static responseFormats = ['json', 'xml']

	def dataSource

	def sqlBuilderService

	def qwerydb() {
		switch(params.mode) {
			case 'data':
				querydbData(params)
				break
			case 'station':
				querydbStation(params)
				break
			case 'magnitude':
				querydbMagnitude(params)
				break
		}
	}

	final int maxRecords = 180000

	def querydbData(params) {
		String filename = "remmaqvisor_data_${new Date().format('yyyymmdd-HHmm')}.csv"
		def genprops = sqlBuilderService.getSql4Data(params)
		def sql = genprops[1]
		def sqlconn = new Sql(dataSource)
		def outs = response.outputStream
		response.status = OK.value()
		response.contentType = "${csvMimeType};charset=${encoding}";
		response.setHeader "Content-disposition", "attachment; filename=${filename}"
		def contador = 0
		outs << "${genprops[0]}\n"
		// sql + + " limit ${maxRecords}"
		sqlconn.eachRow(sql) { row ->
			def line =  "${row.date?row.date.toString().substring(0,16):''};" +
				"${row.station?:''};" +
				"${row.magnitude?:''};" +
				"${row.abbr?:''};" +
				"${row.unit?:''};" +
				"${row.minute?:''};" +
				"${row['10mins']?:''};" +
				"${row.hour?:''};" +
				"${row['8hours']?:''};" +
				"${row['24hours']?:''};" +
				"${row['72hours']?:''};\n"
			outs << line
			contador++
			if(contador % 192 == 0)
				outs.flush()
		}
//		if(contador >= maxRecords)
//			outs << "ERROR: Dataset is too large, just up to ${maxRecords} records allowed to be exported to CSV file"
		outs.flush()
		outs.close()
	}

	def querydbStation(params) {
		def sqlconn = new Sql(dataSource)
		def genprops = sqlBuilderService.getSql4Station(params, sqlconn)
		def sql = genprops[1]
		String filename = "remmaqvisor_station_${new Date().format('yyyymmdd-HHmm')}.csv"
		def outs = response.outputStream
		response.status = OK.value()
		response.contentType = "${csvMimeType};charset=${encoding}";
		response.setHeader "Content-disposition", "attachment; filename=${filename}"
		def contador = 0
		outs << "${genprops[0]}\n"
		// sql + " limit ${maxRecords}"
		sqlconn.eachRow(sql) { row ->
			def record = []
			row.getMetaData().columnCount.times { record << row[it] }
			record[1] = record[1]?record[1].toString().substring(0,16):''
			outs << record.join(';').replace('null', '') + '\n'

			contador++
			if(contador % 192 == 0)
				outs.flush()
		}
//		if(contador >= maxRecords)
//			outs << "ERROR: Dataset is too large, just up to ${maxRecords} records allowed to be exported to CSV file"
		outs.flush()
		outs.close()
	}

	def querydbMagnitude(params) {
		def sqlconn = new Sql(dataSource)
		def genprops = sqlBuilderService.getSql4Magnitude(params, sqlconn)
		def sql = genprops[1]
		String filename = "remmaqvisor_magnitude_${new Date().format('yyyymmdd-HHmm')}.csv"
		def outs = response.outputStream
		response.status = OK.value()
		response.contentType = "${csvMimeType};charset=${encoding}"
		response.setHeader "Content-disposition", "attachment; filename=${filename}"
		def contador = 0
		outs << "${genprops[0]}\n"
		// sql+ " limit ${maxRecords}"
		sqlconn.eachRow(sql) { row ->
			def record = []
			row.getMetaData().columnCount.times { record << row[it] }
			record[1] = record[1]?record[1].toString().substring(0,16):''
			outs << record.join(';').replace('null','') + '\n'

			contador++
			if(contador % 192 == 0)
				outs.flush()
		}
//		if(contador >= maxRecords)
//			outs << "ERROR: Dataset is too large, just up to ${maxRecords} records allowed to be exported to CSV file"
		outs.flush()
		outs.close()
	}

	@Override
	void setConfiguration(Config co) {
		csvMimeType = co.getProperty('grails.mime.types.csv', String, 'text/csv')
		encoding = co.getProperty('grails.converters.encoding', String, 'UTF-8')
	}
}
