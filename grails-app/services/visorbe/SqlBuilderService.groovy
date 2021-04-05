package visorbe

import grails.gorm.transactions.Transactional

@Transactional
class SqlBuilderService {

	@Transactional(readOnly=true)
	def getSqlTiming(params) {
		def years = params.years != '' ? " and year in (${params.years})" : ''
		def months = params.years != '' ? " and month in (${params.months})" : ''
		def monthDays = params.monthDays != '' ? " and dom in (${params.monthDays})" : ''
		def hours = params.hours != '' ? " and hour in (${params.hours})" : ''
		def weekdays = params.weekDays != '' ? " and dow in (${params.weekDays})" : ''
        def lines = " minute is not null "
        switch(params.lines) {
            case '2': lines = " minute in (0,10,20,30,40,50,60) "; break
            case '3': lines = " minute = 0 "; break
        }

		def filter = lines + years + months + monthDays + hours + weekdays

		def sql= "(select date from survey.dating where " + filter + ') t '

		return sql
	}

	@Transactional(readOnly=true)
	def getMagnitudes(sqlconn, magnitudes) {
		def sql = "select id, pname, abbreviation, unit from survey.magnitude where id in (" + magnitudes + ") order by abbreviation"
		return sqlconn.rows(sql)
	}

	@Transactional(readOnly=true)
	def getStations(sqlconn, stations) {
		def sql = "select id, pname, pid from survey.opoint where id in (" + stations + ") order by pid"
		return sqlconn.rows(sql)
	}

    @Transactional(readOnly=true)
    def getDataField(params) {
        // '1minute':1, '10minutes':10, '1hour':60, '8hours':8, '24hours':24, '72hours':72
        def fieldname = 'avg1h'
        switch(params.datas) {
            case '1':  fieldname = 'avg1m';  break
//            case '10': fieldname = 'avg10m'; break
            case '60': fieldname = 'avg1h';  break
//            case '8':  fieldname = 'avg8h';  break
//            case '24': fieldname = 'avg24h'; break
//            case '72': fieldname = 'avg72h'; break
        }
        return fieldname
    }

    @Transactional(readOnly=true)
    def getDataFieldTitle(params) {
        // '1minute':1, '10minutes':10, '1hour':60, '8hours':8, '24hours':24, '72hours':72
        def fieldname = '1 hour'
        switch(params.datas) {
            case '1':  fieldname = 'minute';  break
//            case '10': fieldname = '10 minutes'; break
            case '60': fieldname = '1 hour';  break
//            case '8':  fieldname = '8 hours';  break
//            case '24': fieldname = '24 hours'; break
//            case '72': fieldname = '72 hours'; break
        }
        return fieldname
    }

	def getDataFieldN(params) {
		// '1minute':1, '10minutes':10, '1hour':60, '8hours':8, '24hours':24, '72hours':72
		def fieldname = 'avg1h'
		switch(params.datas) {
			case '1':  fieldname = '1';  break
			case '10': fieldname = 'n10m'; break
			case '60': fieldname = 'n1h';  break
			case '8':  fieldname = 'n8h';  break
			case '24': fieldname = 'n24h'; break
			case '72': fieldname = 'n72h'; break
		}
		return fieldname
	}

	// DATA --

	@Transactional(readOnly=true)
    def getSql4Data(params) {
		def sqlTiming = getSqlTiming(params)
		def select = 'select t.date, o.pname "station", m.pname "magnitude", m.abbreviation "abbr", m.unit, d.avg1m "minute", d.avg10m "10mins", d.avg1h "hour" from '
		def sqlData = getSqlData(params)
		def orderBy = ' order by 1, 2, 3'
		return [
            'date;station;magnitude;abbr;unit;minute;10mins;hour',
			select + sqlTiming + sqlData + orderBy
		]
    }

	@Transactional(readOnly=true)
	def getSqlData(params) {
		def magnitudes = params.magnitudes != '' ? " and d.magnitude_id in (${params.magnitudes})" : ''
		def opoints = params.opoints != '' ? " and d.opoint_id in (${params.opoints})" : ''
		def filter = magnitudes + opoints

		def sql= " left join survey.data d on d.datetime=t.date " + filter
		sql = sql + " left join survey.magnitude m on d.magnitude_id = m.id "
		sql = sql + " left join survey.opoint o on d.opoint_id = o.id "

		return sql
	}

	// STATION -- MAGNITUDES

	@Transactional(readOnly=true)
	def getSql4Station(params, sqlconn) {
		def sqlTiming = getSqlTiming(params)
		def magnitudes = getMagnitudes(sqlconn, params.magnitudes)

		def ids = []
		def abbrs = []
		def units = []

		magnitudes.each {item ->
			ids.push(item.id)
			abbrs.push(item.abbreviation)
			units.push(item.unit)
		}

		def station = sqlconn.firstRow("select pname from survey.opoint where id = " + params.opoints.split(',')[0])

		def select = "select '" +station[0] + "' station, t.date"
		magnitudes.each { item ->
			if(params.datas == 1 || item.id != 89)
				select += ", d${item.id}.${getDataField(params)} \"${item.abbreviation}\""
			else
				select += ", d${item.id}.${getDataField(params)} * coalesce(d${item.id}.${getDataFieldN(params)}, 1) \"${item.abbreviation}\""
		}
		select += ' from '

		def sqlData = ''
		ids.each {id ->
			sqlData += " \nLEFT JOIN survey.data d${id} on t.date = d${id}.datetime and d${id}.magnitude_id=${id} " +
				"and d${id}.opoint_id = " + params.opoints[0]
		}
		def orderBy = ' order by 1, 2 '

		return [
			'Station;date;' + abbrs.join(';') + "\n;;" + units.join(';'),
			select + sqlTiming + sqlData + orderBy
		]
	}

	@Transactional(readOnly=true)
	def getSqlMagnitudes(params) {
		def magnitudes = params.magnitudes != '' ? " and d.magnitude_id in (${params.magnitudes})" : ''
		def opoints = params.opoints != '' ? " and d.opoint_id in (${params.opoints})" : ''
		def filter = magnitudes + opoints

		def sql= "left join survey.data d on d.datetime=t.date" + filter
		sql = sql + " left join survey.magnitude m on d.magnitude_id = m.id "
		sql = sql + " left join survey.opoint o on d.opoint_id = o.id "

		return sql
	}

	// MAGNITUD -- STATIONS

	@Transactional(readOnly=true)
	def getSql4Magnitude(params, sqlconn) {
		def magnitudes = params.magnitudes.split(',')
		def sqlTiming = getSqlTiming(params)
		def stations = getStations(sqlconn, params.opoints)
		def ids = []
		def pnames = []
		def pids = []
		stations.each {item ->
			ids.push(item.id)
			pnames.push(item.pname)
			pids.push(item.pid)
		}

		def magnitude = sqlconn.firstRow("select pname from survey.magnitude where id = " + magnitudes[0])
        def fieldName = getDataField(params)
		def fieldNameN = getDataFieldN(params)
        def fieldNameTitle = getDataFieldTitle(params)

		def select = "select '" + magnitude.pname + "' magnitude, t.date"
		stations.each { item ->
			if(params.datas == 1 || magnitudes[0] != '89')
				select += ", d${item.id}.${fieldName} \"${item.pid}\""
			else
			    select += ", d${item.id}.${fieldName} * coalesce(d${item.id}.${fieldNameN}, 1) \"${item.pid}\""
		}
		select += ' from '

//		println "5 =======================================>"
//		println select
//		println "6 =======================================>"

		def sqlData = ''
		ids.each {id ->
			sqlData += " \nLEFT JOIN survey.data d${id} on t.date = d${id}.datetime and d${id}.opoint_id=${id} " +
				"and d${id}.magnitude_id = " + magnitudes[0]
		}
		def orderBy = ' order by 1, 2 '

        return [
			'Magnitude;date;' + pids.join(';') + '\n' + pids.inject(';') {str, it -> str + ";$fieldNameTitle"},
			select + sqlTiming + sqlData + orderBy
		]
	}
}
