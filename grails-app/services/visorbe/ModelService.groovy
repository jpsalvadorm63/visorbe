package visorbe

import grails.gorm.transactions.Transactional
import groovy.sql.Sql

@Transactional
class ModelService {
	static scope = "singleton"
	def dataSource
	def sqlconn
	def opoints = []
	def opoints2 = [
		[id:  4, pid: 'JIP', opoint_name: 'Jipijapa', nasa_name: null],
		[id:  5, pid: 'CML', opoint_name: 'El Camal', nasa_name: 'Quito_El_Camal'],
		[id:  1, pid: 'COT', opoint_name: 'Cotocollao', nasa_name: 'Quito_Cotocollao'],
		[id:  7, pid: 'GUA', opoint_name: 'Guamani', nasa_name: 'Quito_Guamani'],
		[id:  8, pid: 'TUM', opoint_name: 'Tumbaco', nasa_name: 'Quito_Tumbaco'],
		[id:  9, pid: 'LCH', opoint_name: 'Los Chillos', nasa_name: 'Quito_LosChillos'],
		[id:  2, pid: 'CAR', opoint_name: 'Carapungo', nasa_name: 'Quito_Carapungo'],
		[id:  3, pid: 'BEL', opoint_name: 'Belisario', nasa_name: 'Quito_Belisario'],
		[id:  6, pid: 'CEN', opoint_name: 'Centro', nasa_name: 'Quito_Centro'],
		[id: 14, pid: 'SAP', opoint_name: 'San Antonio', nasa_name: null],
		[id: 41, pid: 'ITC', opoint_name: 'ITCHIMBIA', nasa_name: null],
		[id: 31, pid: 'CUA', opoint_name: 'LAS CUADRAS', nasa_name: null],
		[id: 51, pid: 'BIC', opoint_name: 'BICENTENARIO', nasa_name: null]
	]
	def opoints4OpenAQ = []
	def magnitudes = []

	def readOpoints() {
		opoints = []
		sqlconn.rows('select * from survey.opoint').each {row ->
			def opoint = [:]
			opoint['id'] = row.id
			opoint['pid'] = row.pid
			opoint['pname'] = row.pname
			opoint['pname2'] = row.pname?.toLowerCase()
			opoint['pname3'] = row.pname?.toLowerCase()?.split(' ')?.join()
			opoint['pname_nasa'] = row.pname_nasa
			opoints << opoint
		}
	}

	def openAQMagnitudes = [
		[magnitude:"PM 2.5", original_unit:"µg/m³", factor:null,       unit:"µg/m³", magnitude_id:10, averagingPeriod:["value": 1, "unit":"hours"]],
		[magnitude:"PM 10",  original_unit:"µg/m³", factor:null,       unit:"µg/m³", magnitude_id:3,  averagingPeriod:["value":24, "unit":"hours"]],
		[magnitude:"CO",     original_unit:"µg/m³", factor:1.1449,     unit:"ppm",   magnitude_id:6,  averagingPeriod:["value": 8, "unit":"hours"]],
		[magnitude:"NO2",    original_unit:"µg/m³", factor:1.88042287, unit:"ppm",   magnitude_id:8,  averagingPeriod:["value": 1, "unit":"hours"]],
		[magnitude:"SO2",    original_unit:"µg/m³", factor:2.6185436,  unit:"ppm",   magnitude_id:1,  averagingPeriod:["value":24, "unit":"hours"]],
		[magnitude:"O3",     original_unit:"µg/m³", factor:null,       unit:"µg/m³", magnitude_id:14, averagingPeriod:["value": 1, "unit":"hours"]]
	]

	def allowedIntervals = [
		'1 hour',   '-1 hour',
		'2 hours',   '-2 hours',
		'4 hours',   '-4 hours',
		'8 hours',  '-8 hours',
		'24 hours', '-24 hours',
		'72 hours', '-72 hours',
		'1 week',   '-1 week',
		'1 month',  '-1 month'
	]

	def readOpoints4OpenAQ() {
		sqlconn.rows("select id, pid abbr, pname loc, x, y, srid from survey.opoint where pid in ('CML','COT','GUA','TUM','LCH','SAP','ITC','CAR','JIP','BEL','CEN');").each { row ->
			def opoint = [:]
			opoint['id'] = row.id
			opoint['abbr'] = row.abbr
			opoint['location'] = row.loc
			opoint['x'] = row.x
			opoint['y'] = row.y
			opoint['srid'] = row.srid
			opoint['city'] = 'Quito'
			opoint['country'] = 'EC'
			opoint['sourceType'] = 'government'
			opoint['sourceName'] = 'IMQ-AMBIENTE-REMMAQ'
			opoint['mobile'] = 'false'
			opoint['coordinates'] = [latitude:row.y, longitude:row.x]
			opoints4OpenAQ << opoint
		}
	}

	def readMagnitudes() {
		sqlconn.rows('select * from survey.magnitude').each {row ->
			def magnitude = [:]
			magnitude['id'] = row.id
			magnitude['pname'] = row.pname
			magnitude['abbreviation'] = row.abbreviation
			magnitude['pname_nasa'] = row.pname_nasa
			magnitudes << magnitude
		}
	}

	def findOpoint(String opoint) {
		if(opoint != null) {
			def myopoint = opoints4OpenAQ.find {it?.location?.toUpperCase() == opoint?.toUpperCase() || it?.abbr?.toUpperCase() == opoint?.toUpperCase()}
			return myopoint
		} else {
			opoints4OpenAQ.collect {[
				abbr:it.abbr,
				location:it.loc,
				x:it.x,
				y:it.y,
				srid:it.srid,
				city:it.city,
				country:it.country,
				sourceType:it.sourceType,
				sourceName:it.sourceName,
				mobile:it.mobile,
				coordinates:it.coordinates
			]}
		}
	}

	def conn() { sqlconn }

	def findIdxInRanges(ArrayList ranges, Double value) {
		if(ranges == null || value == null || (ranges.size() == 0) || (ranges.size() == 1))
			return null
		else {
			Integer idx = null
			if(value < (ranges[0] as double)) return null
			if(value == (ranges[0] as double)) return 0
			if(value < (ranges[0] as double)) return null
			if(value > (ranges[ranges.size - 1] as double)) return (ranges.size - 2)
			ranges.eachWithIndex{ int entry, int i ->
				if(i < (ranges.size() - 1) && ranges[i] < value && ranges[i+1] >= value)
					idx = i
			}
			return idx
		}
	}

	def magnitudeById(id) { magnitudes.find {it -> it.id == id} }

	def presentation = [
			10:[code:'data',   es:'Data',                                     en:'Data'],
			20:[code:'minmax', es:'Mínimo y Máximo',                          en:'Min and Max'],
			30:[code:'data2',  es:'Dato de magnitud asociada',                en:'Asociated magnitude data'],
			40:[code:'IQCA',   es:'Indice Quiteño de Calidad del Aire',       en:'Quito City Air Quality Index'],
			50:[code:'AQI',    es:'Indice Internacional de Calidad del Aire', en:'International Air Quality Index'],
			60:[code:'other',  es:'Otra concentración',                       en:'Another concentration']
	]

	def calcDATA24h(prms) { return prms?.value24h }

	def calcIQCA24h(prms) { return (prms.isHourlyData==null || prms.isHourlyData)?prms.value24h:prms.value24max?prms.value24max:prms.value24h}

	def AQIids() { [0 , 1 , 2 , 3 , 4 , 5 , 6] }

	def AQIname(id, lang = 'en') {
		[
			0: (lang=='en')?'good':'bueno',
			1: (lang=='en')?'moderate':'moderado',
			2: (lang=='en')?'unhealthy sensitive groups':'insalubre grupos sensibles',
			3: (lang=='en')?'unhealthy':'insalubre',
			4: (lang=='en')?'very unhealthy':'muy insalubre',
			5: (lang=='en')?'hazardous':'peligroso',
			6: (lang=='en')?'very hazardous':'muy peligroso'
		]
	}

	def AQIcolors(Integer id) {
		[
			0: 'rgb(50, 205, 50)',
			1: 'rgb(255, 255, 0)',
			2: 'rgb(255, 165, 0)',
			3: 'rgb(255, 0, 0)',
			4: 'rgb(148, 0, 211)',
			5: 'rgb(138, 0, 0)',
			6: 'rgb(128, 0, 0)'
		][id]
	}



	def IQCA_good = 0
	def IQCA_acceptable = 1
	def IQCA_caution = 2
	def IQCA_alert = 3
	def IQCA_alarm = 4
	def IQCA_emergency = 5
	def IQCA_very_danger = 6

	def IQCAcolors(Integer id) {
		[
			0: 'rgb(135, 206, 250)',
			1: 'rgb(30, 144, 255)',
			2: 'rgb(50, 205, 50)',
			3: 'rgb(255, 255, 0)',
			4: 'rgb(255, 165, 0)',
			5: 'rgb(255, 0, 0)',
			6: 'rgb(148, 0, 211)'
		][id]
	}

	def IQCids() {[0, 1, 2, 3, 4, 5, 6]}

	def magnitude_schemas = [
		[
			magnitude_id: 1,
			magnitude_name: [es:'Dióxido de Azufre [SO2]', en:'Sulfur dioxide [SO2]'],
			nasa_name: null,
			unit: 'ug/m3',
			DATA: [
				value: { prms -> calcDATA24h(prms) },
				colDescription: {lang -> (lang == 'es')?['24hC','Concentración promedio últimas 24 horas']:['24hC','last 24 hours average concentration']},
				valuemin: { prms -> prms?.value24min },
				valuemed: { prms -> prms?.value24med },
				valuemax: { prms -> prms?.value24max }
			],
			CONCENTRATION: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['1hC','Concentración promedio en 1 hora']:['1hC','1 hour average concentration']},
				valuemin: { prms -> prms?.value1min },
				valuemed: { prms -> prms?.value1med },
				valuemax: { prms -> prms?.value1max }
			],
			IQCA: [
				value: { prms ->
					def d24h = (prms.isHourlyData==null || prms.isHourlyData)?prms.value24h:prms.value24max?prms.value24max:prms.value24h
					def result = null
					if(d24h >= 0 && d24h < 62.5)         result =  0.80*d24h
					else if(d24h >= 62.5 && d24h < 125)  result =  0.80*d24h
					else if(d24h >= 125  && d24h < 200)  result =  4*d24h/3.0 -200/3.0
					else if(d24h >= 200  && d24h < 1000) result =  0.125*d24h + 175
					else if(d24h >= 1000 && d24h < 1800) result =  0.125*d24h + 175
					else if(d24h >= 1800)                result =  0.125*d24h + 175
					else result =  null
					return result
				},
				colDescription: { lang -> (lang == 'es') ? ['IQCA', 'Indice Quiteño de la Calidad del Aire'] : ['IQCA', 'Aire Quality Quito Index'] },
				description: { lang -> (lang == 'es') ? ['Índice Quiteño de Calidad del Aire', 'IQCA'] : ['Quito Air Quality Index', 'IQCA'] },
				decimals: 1
			],
			AQI: [
				value: {
					prms ->
						def d24h = (prms.isHourlyData==null || prms.isHourlyData) ? prms.value24h : prms.value24max ? prms.value24max : prms.value24h
						if (d24h >= 0 && d24h < 91.7) return (50 / 91.7) * d24h
						else if (d24h >= 91.7 && d24h < 196.5) return 0.47710 * d24h + 6.25
						else if (d24h >= 196.5 && d24h < 484.7) return 0.17349 * d24h + 65.90909
						else if (d24h >= 484.7 && d24h < 796.48) return 0.16037 * d24h + 72.26891
						else if (d24h >= 796.48 && d24h < 1582.48) return 0.12722 * d24h + 98.66667
						else if (d24h >= 1582.48 && d24h < 2106.48) return 0.19084 * d24h - 2
						else if (d24h >= 2106.48 && d24h < 2630.48) return 0.19084 * d24h - 2
						else return null
				},
				colDescription: { lang -> (lang == 'es') ? ['AQI', 'Indice Internacional de la Calidad del Aire'] : ['AQI', 'Aire Quality Index'] },
				description: { lang -> (lang == 'es') ? ['Índice Internacional de Calidad del Aire', 'AQI'] : ['Air Quality Index', 'AQI'] },
				ranges: [0, 35, 75, 185, 304, 604, 804, 1004],
				colors: AQIcolors,
				decimals: 1
			],
		],
		[
			magnitude_id: 3,
			magnitude_name: [es:'Material Particulado 10 [PM10]', en:'Particulate material 10 [PM10]'],
			nasa_name: null,
			unit: 'ug/m3',
			DATA: [
				value: { prms -> prms?.value24h },
				colDescription: {lang -> (lang == 'es')?['24hC','Concentración promedio últimas 24 horas']:['24hC','last 24 hours average concentration']},
				valuemin: { prms -> prms?.value24min },
				valuemed: { prms -> prms?.value24med },
				valuemax: { prms -> prms?.value24max }
			],
			CONCENTRATION: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['1hC','Concentración promedio en 1 hora']:['1hC','1 hour average concentration']},
				valuemin: { prms -> prms?.value1min },
				valuemed: { prms -> prms?.value1med },
				valuemax: { prms -> prms?.value1max }
			],
			IQCA: [
				value: { prms ->
					def d24h = (prms.isHourlyData==null || prms.isHourlyData)?prms.value24h:prms.value24max?prms.value24max:prms.value24h
					if(d24h >= 0 && d24h <= 50)          return 1.0*d24h
					else if(d24h > 50 && d24h <= 100)    return 1.0*d24h
					else if(d24h > 100  && d24h <= 250)  return 2*d24h/3.0 + (100/3.0)
					else if(d24h > 250  && d24h <= 400)  return 2*d24h/3.0 + (100/3.0)
					else if(d24h > 400 && d24h <= 500)   return 1*d24h - 100
					else if(d24h > 500)                  return 1*d24h - 100
					else return null
				},
				colDescription: { lang -> (lang == 'es') ? ['IQCA', 'Indice Quiteño de la Calidad del Aire'] : ['IQCA', 'Aire Quality Quito Index'] },
				description: { lang -> (lang == 'es' ? ['Índice Quiteño de Calidad del Aire', 'IQCA'] : ['Quito Air Quality Index', 'IQCA']) },
				decimals: 1
			],
			AQI: [
				value: {prms ->
					def d24h = (prms.isHourlyData==null || prms.isHourlyData)?prms.value24h:prms.value24max?prms.value24max:prms.value24h
					if(d24h >= 0 && d24h < 54)          return (25/27.0)*d24h
					else if(d24h >= 54 && d24h < 154)  return 0.5*d24h + 23
					else if(d24h >= 154 && d24h < 254) return 0.5*d24h + 23
					else if(d24h >= 254 && d24h < 354) return 0.5*d24h + 23
					else if(d24h >= 354 && d24h < 424) return 1.42857*d24h - 305.71428
					else if(d24h >= 424 && d24h < 504) return 1.25*d24h - 230
					else if(d24h >= 504 && d24h < 604) return 1.00*d24h - 104
					else
						return null
				},
				colDescription: { lang -> (lang == 'es') ? ['AQI', 'Indice Internacional de la Calidad del Aire'] : ['AQI', 'Aire Quality Index'] },
				description: { lang -> (lang == 'es' ? ['Índice Internacional de Calidad del Aire', 'AQI'] : ['Air Quality Index', 'AQI']) },
				ranges: [0, 54, 154, 254, 354, 424, 504, 604],
				colors: AQIcolors,
				decimals: 1
			]
		],
		[
			magnitude_id: 6,
			magnitude_name: [es:'Monóxido de Carbono [CO]', en:'Carbon monoxide [CO]'],
			nasa_name: null,
			unit: 'ug/m3',
			DATA: [
				value: { prms -> prms?.value8h },
				colDescription: {lang -> (lang == 'es')?['8hC','Concentración promedio en 8 horas']:['8hC','8 hours average concentration']},
				valuemin: { prms -> prms?.value8min },
				valuemed: { prms -> prms?.value8med },
				valuemax: { prms -> prms?.value8max }
			],
			CONCENTRATION: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['1hC','Concentración promedio en 1 hora']:['1hC','1 hour average concentration']},
				valuemin: { prms -> prms?.value1min },
				valuemed: { prms -> prms?.value1med },
				valuemax: { prms -> prms?.value1max }
			],
			IQCA: [
				value: { prms ->
					def d8h = (prms.isHourlyData==null || prms.isHourlyData) ? prms.value8h : prms.value8max ? prms.value8max : prms.value8h
					if (d8h >= 0 && d8h <= 5) return 10 * d8h
					else if (d8h > 5 && d8h <= 10) return 10 * d8h
					else if (d8h > 10 && d8h <= 15) return 20 * d8h - 100
					else if (d8h > 15 && d8h <= 30) return 6.67 * d8h + 100
					else if (d8h > 30 && d8h <= 40) return 10 * d8h
					else if (d8h > 40) return 10 * d8h
					else return null
				},
				colDescription: { lang -> (lang == 'es') ? ['IQCA', 'Indice Quiteño de la Calidad del Aire'] : ['IQCA', 'Aire Quality Quito Index'] },
				description: { lang -> (lang == 'es' ? ['Índice Quiteño de Calidad del Aire', 'IQCA'] : ['Quito Air Quality Index', 'IQCA']) },
				decimals: 0
			],
			AQI: [
				value: {prms ->
					def d8h = (prms.isHourlyData==null || prms.isHourlyData)?prms.value8h:prms.value8max?prms.value8max:prms.value8h
					if(d8h >= 0          && d8h <= 5.038)  return (50/5.038)*d8h
					else if(d8h > 5.038  && d8h <= 10.763) return 8.73362*d8h + 6
					else if(d8h > 10.763 && d8h <= 14.198) return 14.556*d8h - 56.667
					else if(d8h > 14.198 && d8h <= 17.633) return 14.556*d8h - 56.667
					else if(d8h > 17.633 && d8h <= 34.808) return 5.82241*d8h + 97.33333
					else if(d8h > 34.808 && d8h <= 46.258) return 8.73362*d8h - 4
					else if(d8h > 46.258 && d8h <= 57.708) return 8.73362*d8h - 4
					else return null
				},
				colDescription: { lang -> (lang == 'es') ? ['AQI', 'Indice Internacional de la Calidad del Aire'] : ['AQI', 'Aire Quality Index'] },
				description: { lang -> (lang == 'es' ? ['Índice Internacional de Calidad del Aire', 'AQI'] : ['Air Quality Index', 'AQI']) },
				ranges: [0, 4.4, 9.4, 12.4, 15.4, 30.4, 40.4, 50.4],
				colors: AQIcolors,
				decimals: 0
			]
	    ],
		[
			magnitude_id: 8,
			magnitude_name: [es:'Dióxido de Nitrógeno [NO2]', en:'Nitrogen dioxide [NO2]'],
			nasa_name: 'no2_ML_[ugm-3]',
			unit: 'ug/m3',
			DATA: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['1hC','Concentración promedio en 1 hora']:['1hC','1 hour average concentration']},
				valuemin: { prms -> prms?.value1min },
				valuemed: { prms -> prms?.value1med },
				valuemax: { prms -> prms?.value1max }
			],
			DATA2: null,
			CONCENTRATION: [
					value: { prms -> prms?.value1 },
					colDescription: {lang -> (lang == 'es')?['1hC','Concentración promedio en 1 hora']:['1hC','1 hour average concentration']},
					valuemin: { prms -> prms?.value1min },
					valuemed: { prms -> prms?.value1med },
					valuemax: { prms -> prms?.value1max }
			],
			IQCA: [
				value: {prms ->
					def d1h = prms.value1
					if (d1h >= 0 && d1h <= 100)         return 0.50 * d1h
					else if (d1h > 100 && d1h <= 200)   return 0.50 * d1h
					else if (d1h > 200 && d1h <= 1000)  return 0.125 * d1h + 75
					else if (d1h > 1000 && d1h <= 2000) return 0.10 * d1h + 100
					else if (d1h > 2000 && d1h <= 3000) return 0.10 * d1h + 100
					else if (d1h > 3000)                return 0.10 * d1h + 100
					else return null
				},
				colDescription: { lang -> (lang == 'es') ? ['IQCA', 'Indice Quiteño de la Calidad del Aire'] : ['IQCA', 'Aire Quality Quito Index'] },
				description: { lang -> (lang == 'es' ? ['Índice Quiteño de Calidad del Aire', 'IQCA'] : ['Quito Air Quality Index', 'IQCA']) },
				decimals: 0
			],
			AQI: [
				value: {prms ->
					def d1h = prms.value1
					if(d1h > 0 && d1h <= 99.64)              return 0.50181*d1h
					else if(d1h > 99.64 && d1h <= 188)       return 0.56587*d1h - 6.38298
					else if(d1h > 188 && d1h <= 676.8)       return 0.10229*d1h + 80.76923
					else if(d1h > 676.8 && d1h <= 1220.12)   return 0.09202*d1h + 87.71626
					else if(d1h > 1220.12 && d1h <= 2348.12) return 0.08865*d1h + 91.83333
					else if(d1h > 2348.12 && d1h <= 3100.12) return 0.13298*d1h - 12.25
					else if(d1h > 3100.12 && d1h <= 3852.12) return 0.13298*d1h - 12.25
					else return null
				},
				colDescription: { lang -> (lang == 'es') ? ['AQI', 'Indice Internacional de la Calidad del Aire'] : ['AQI', 'Aire Quality Index'] },
				description: { lang -> (lang == 'es' ? ['Índice Internacional de Calidad del Aire', 'AQI'] : ['Air Quality Index', 'AQI']) },
				ranges: [0, 53, 100, 360, 649, 1249, 1649, 20149],
				colors: AQIcolors,
				decimals: 0
			]
		],
		[
			magnitude_id: 10,
			magnitude_name: [es:'Material Particulado 2.5 [PM2.5]', en:'Particulate material [PM2.5]'],
			nasa_name: 'pm25_gcc_ML_[ugm-3]',
			unit: 'ug/m3',
			DATA: [
				value: { prms -> prms?.value24h },
				colDescription: {lang -> (lang == 'es')?['24hC','Concentración promedio últimas 24 horas']:['24hC','last 24 hours average concentration']},
				valuemin: {prms -> prms?.value24min},
				valuemed: { prms -> prms?.value24med },
				valuemax: { prms -> prms?.value24max }
			],
			CONCENTRATION: [
				value: { prms -> prms.value1 },
				colDescription: {lang -> (lang == 'es')?['1hC','Concentración promedio en 1 hora']:['1hC','1 hour average concentration']},
				valuemin: { prms -> prms?.value1min },
				valuemed: { prms -> prms?.value1med },
				valuemax: { prms -> prms?.value1max }
			],
			IQCA: [
				value: { prms ->
					def d24h = (prms.isHourlyData==null || prms.isHourlyData)?prms.value24h:prms.value24max?prms.value24max:prms.value24h
					if(d24h >= 0 && d24h <= 25)         return 2.0*d24h
					else if(d24h > 25 && d24h <= 50)    return 2.0*d24h
					else if(d24h > 50  && d24h <= 150)  return 1*d24h + 50
					else if(d24h > 150  && d24h <= 250) return 1*d24h + 50
					else if(d24h > 250 && d24h <= 350)  return 1*d24h + 50
					else if(d24h > 350)                 return 1*d24h + 50
					else return null
				},
				colDescription: { lang -> (lang == 'es') ? ['IQCA', 'Indice Quiteño de la Calidad del Aire'] : ['IQCA', 'Aire Quality Quito Index'] },
				description: { lang -> (lang == 'es') ? ['Índice Quiteño de Calidad del Aire', 'IQCA'] : ['Quito Air Quality Index', 'IQCA'] },
				decimals: 1
			],
			AQI: [
				value: { prms ->
					def d24h = (prms.isHourlyData==null || prms.isHourlyData)?prms.value24h:prms.value24max?prms.value24max:prms.value24h
					if(d24h >= 0 && d24h <= 12)            return (25/6.0)*d24h
					else if(d24h > 12 && d24h <= 35.4)     return 2.13675*d24h + 24.35897
					else if(d24h > 35.4 && d24h <= 55.4)   return 2.50*d24h + 11.50
					else if(d24h > 55.4 && d24h <= 150.4)  return 0.52631*d24h + 120.84210
					else if(d24h > 150.4 && d24h <= 250.4) return 1.00*d24h + 49.6
					else if(d24h > 250.4 && d24h <= 350.4) return 1.00*d24h + 49.6
					else if(d24h > 350.4 && d24h <= 500.4) return (2/3)*d24h + 166.4
					else return null
				},
				colDescription: { lang -> (lang == 'es') ? ['AQI', 'Indice Internacional de la Calidad del Aire'] : ['AQI', 'Aire Quality Index'] },
				description: { lang -> (lang == 'es' ? ['Índice Internacional de Calidad del Aire', 'AQI'] : ['Air Quality Index', 'AQI']) },
				ranges: [0, 12, 35.4, 55.4, 150.4, 250.4, 350.4, 500.4],
				colors: AQIcolors,
				decimals: 1
			]
		],
		[
			magnitude_id: 14,
			magnitude_name: [es:'Ozono [O3]', en:'Ozone [O3]'],
			nasa_name: 'o3_ML_[ugm-3]',
			unit: 'ug/m3',
			DATA: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['1hC','Concentración promedio en 1 hora']:['1hC','1 hour average concentration']},
				valuemin: { prms -> prms?.value1min },
				valuemed: { prms -> prms?.value1med },
				valuemax: { prms -> prms?.value1max }
			],
			CONCENTRATION: [
				value: { prms -> prms?.value8h },
				desc: ['c8h', '8 hours concentration','concentración 8 horas'],
				valuemin: { prms -> prms?.value8min },
				valuemed: { prms -> prms?.value8med },
				valuemax: { prms -> prms?.value8max }
			],
			IQCA: [
				value: {prms ->
					def d1h = (prms.isHourlyData==null || prms.isHourlyData)?prms.value1:prms.value1max?prms.value1max:prms.value1
					if (d1h >= 0 && d1h <= 50) return 1.0 * d1h
					else if (d1h > 50 && d1h <= 100) return 1.0 * d1h
					else if (d1h > 100 && d1h <= 200) return 1.0 * d1h
					else if (d1h > 200 && d1h <= 400) return 0.5 * d1h + 100
					else if (d1h > 400 && d1h <= 600) return 0.5 * d1h + 100
					else if (d1h > 600) return 0.5 * d1h + 100
					else return null
				},
				colDescription: { lang -> (lang == 'es') ? ['IQCA', 'Indice Quiteño de la Calidad del Aire'] : ['IQCA', 'Aire Quality Quito Index'] },
				description: { lang -> (lang == 'es' ? ['Índice Quiteño de Calidad del Aire', 'IQCA'] : ['Quito Air Quality Index', 'IQCA']) },
				decimals: 1,
				colors: AQIcolors,
			],
			AQI: [
				value: {prms ->
					def d8h = (prms.isHourlyData==null || prms.isHourlyData)?prms.value8h:prms.value8max?prms.value8max:prms.value8h
					println "11 =-> ${prms}"
					println "12 =-> ${prms.value1}"
					println "13 =-> ${prms.value1max}"
					def d1h = (prms.isHourlyData==null || prms.isHourlyData)?prms.value1:prms.value1max?prms.value1max:prms.value1
					if (d8h >= 0 && d8h <= 108) return (50.0 / 108) * d8h
					else if (d8h > 108 && d8h <= 140) return 1.5625 * d8h - 115.75
					else if (d8h > 140 && d8h <= 170) return 1.6667 * d8h - 133.3333
					else if (d8h > 170 && d8h <= 210) return 1.25 * d8h - 62.5
					else if (d8h > 210 && d8h <= 400) return 0.526316 * d8h + 89.47368
					else if (d8h > 400) return 1.0 * d8h
					else return null
				},
				colDescription: { lang -> (lang == 'es') ? ['AQI', 'Indice Internacional de la Calidad del Aire'] : ['AQI', 'Aire Quality Index'] },
				description: { lang -> (lang == 'es' ? ['Índice Internacional de Calidad del Aire', 'AQI'] : ['Air Quality Index', 'AQI']) },
				scale: [0, 54, 70, 85, 105, 200, 303, 556], /* 303 & 556 al calculated from hourly*/
				ranges: [0, 12, 35.4, 55.4, 150.4, 250.4, 350.4, 500.4],
				decimals: 1
			]
		],
		[
			magnitude_id: 7,
			magnitude_name: [es:'Monóxido de Nitrógeno [NO]', en:'Nitrogen monoxide [NO]'],
			nasa_name: null,
			unit: 'ug/m3',
			DATA: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['1hC','Concentración promedio en 1 hora']:['1hC','1 hour average concentration']},
				valuemin: { prms -> prms?.value1min },
				valuemed: { prms -> prms?.value1med },
				valuemax: { prms -> prms?.value1max }
			],
			CONCENTRATION: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['1hC','Concentración promedio en 1 hora']:['1hC','1 hour average concentration']},
				valuemin: { prms -> prms?.value1min },
				valuemed: { prms -> prms?.value1med },
				valuemax: { prms -> prms?.value1max }
			],
		],
		[
			magnitude_id: 12,
			magnitude_name: [es:'Óxido de Nitrógeno [NOx]', en:'Nitrogen oxide [NOx]'],
			unit: 'ug/m3',
			DATA: [
				value: {prms -> prms?.value1},
				colDescription: {lang -> (lang == 'es')?['1hC','Concentración promedio en 1 hora']:['1hC','1 hour average concentration']},
				valuemin: {prms -> prms?.value1min},
				valuemed: { prms -> prms?.value1med },
				valuemax: {prms -> prms?.value1max}
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1},
				colDescription: {lang -> (lang == 'es')?['1hC','Concentración promedio en 1 hora']:['1hC','1 hour average concentration']},
				valuemin: {prms -> prms?.value1min},
				valuemed: { prms -> prms?.value1med },
				valuemax: {prms -> prms?.value1max}
			]
		],
		[
			magnitude_id: 88,
			magnitude_name: [es:'Radiación Solar', en:'Solar radiation'],
			nasa_name: null,
			unit: 'W/m2',
			DATA: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			]
		],
		[
			magnitude_id: 99,
			magnitude_name: [es:'Indice Radiación UV', en:'UV radiation index [IUV]'],
			unit: '',
			DATA: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			]
		],
		[
			magnitude_id: 201,
			magnitude_name: [es: 'Irradiancia UV', en:'UV Radiance'],
			nasa_name: null,
			unit: 'W/m2',
			DATA: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			]
		],
		[
			magnitude_id: 93,
			magnitude_name: [es:'Ed305', en:'Ed305'],
			unit: 'uW/(cm2nm)',
			DATA: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			]
		],
		[
			magnitude_id: 94,
			magnitude_name: [es:'Ed313', en:'Ed313'],
			nasa_name: null,
			unit: 'uW/(cm2nm)',
			DATA: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			]
		],
		[
			magnitude_id: 95,
			magnitude_name: [es:'Ed320', en:'Ed320'],
			unit: 'uW/(cm2nm)',
			DATA: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			]
		],
		[
			magnitude_id: 96,
			magnitude_name: [es:'Ed340', en:'Ed340'],
			nasa_name: null,
			unit: 'uW/(cm2nm)',
			DATA: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			]
		],
		[
			magnitude_id: 97,
			magnitude_name: [es:'Ed380', en:'Ed380'],
			nasa_name: null,
			unit: 'uW/(cm2nm)',
			DATA: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			],
			DATA2: null,
			CONCENTRATION: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			]
		],
		[
			magnitude_id: 98,
			magnitude_name: [es:'Ed395', en:'Ed395'],
			nasa_name: null,
			unit: 'uW/(cm2nm)',
			DATA: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			],
			DATA2: null,
			CONCENTRATION: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			]
		],
		[
			magnitude_id: 100,
			magnitude_name: [es:'PAR', en:'PAR'],
			nasa_name: null,
			unit: 'uE/(cm2seg)',
			DATA: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			],
			DATA2: null,
			CONCENTRATION: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			]
		],
		[
			magnitude_id: 81,
			magnitude_name: [es:'Velocidad viento', en:'Wind speed'],
			nasa_name: 'wind_speed_[ms-1]',
			unit: 'm/s',
			DATA: [
				value: {prms -> prms?.value1},
				valuex: {prms -> prms?.value1x},
				valuey: {prms -> prms?.value1y},
				colDescription: {lang -> (lang == 'es')?['1hP','promedio 1 hora']:['1hP','1 hour average']},
				valuemax: {prms -> prms?.value1max},
				valuemin: null,
				valuemed: null
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1},
				valuex: {prms -> prms?.value1x},
				valuey: {prms -> prms?.value1y},
				colDescription: {lang -> (lang == 'es')?['1hP','promedio 1 hora']:['1hP','1 hour average']},
				valuemax: {prms -> prms?.value1max},
				valuemin: null,
				valuemed: null
			]
		],
		[
			magnitude_id: 82,
			magnitude_name: [es:'Dirección viento', en:'Wind dirección'],
			nasa_name: 'wind_direction',
			unit: '\260',
			DATA: [
				value: {prms -> prms?.value1},
				valuex: {prms -> prms?.value1x},
				valuey: {prms -> prms?.value1y},
				colDescription: {lang -> (lang == 'es')?['1hP','promedio 1 hora']:['1hP','1 hour average']},
				valuemax: null,
				valuemin: null,
				valuemed: null
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1},
				valuex: {prms -> prms?.value1x},
				valuey: {prms -> prms?.value1y},
				colDescription: {lang -> (lang == 'es')?['1hP','promedio 1 hora']:['1hP','1 hour average']},
				valuemax: null,
				valuemin: null,
				valuemed: null
			]
		],
		[
			magnitude_id: 83,
			magnitude_name: [es:'Temperatura Media', en:'Average temperature'],
			nasa_name: 'Temperature_[K]',
			unit: 'oC',
			DATA: [
				value: {prms -> prms?.value1},
				colDescription: {lang -> (lang == 'es')?['1hP','promedio 1 hora']:['1hP','1 hour average']},
				valuemin: {prms -> prms?.value1min},
				valuemed: { prms -> prms?.value1med },
				valuemax: {prms -> prms?.value1max}
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1},
				colDescription: {lang -> (lang == 'es')?['1hP','promedio 1 hora']:['1hP','1 hour average']},
				valuemin: {prms -> prms?.value1min},
				valuemed: { prms -> prms?.value1med },
				valuemax: {prms -> prms?.value1max}
			]
		],
		[
			magnitude_id: 86,
			magnitude_name: [es:'Humedad relativa', en:'Relative humidity'],
			nasa_name: null,
			unit: '%',
			DATA: [
				value: {prms -> prms?.value1},
				colDescription: {lang -> (lang == 'es')?['1hP','promedio 1 hora']:['1hP','1 hour average']},
				valuemin: {prms -> prms?.value1min},
				valuemed: { prms -> prms?.value1med },
				valuemax: {prms -> prms?.value1max},
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1},
				colDescription: {lang -> (lang == 'es')?['1hP','promedio 1 hora']:['1hP','1 hour average']},
				valuemin: {prms -> prms?.value1min},
				valuemed: { prms -> prms?.value1med },
				valuemax: {prms -> prms?.value1max},
			]
		],
		[
			magnitude_id: 87,
			magnitude_name: [es:'Presión barométrica', en:'Barometric Pressure'],
			nasa_name: 'SurfacePressure_[hPa]',
			unit: 'mb',
			DATA: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['1hP','promedio 1 hora']:['1hP','1 hour average']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			],
			CONCENTRATION: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['1hP','promedio 1 hora']:['1hP','1 hour average']},
				valuemin: null,
				valuemed: null,
				valuemax: null
			]
		],
		[
			magnitude_id: 89,
			magnitude_name: [es:'Lluvia acumulada', en:'Accumulate rainfall'],
			nasa_name: 'Precipitation_[mm]',
			unit: 'mm',
			DATA: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['1hAc','Total acumulado en 1 hora']:['1hAc','1 hour accumulate']},
				valuemax: { prms -> prms?.value1 },
				valuemin: null,
				valuemed: null
			],
			CONCENTRATION: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['1hAc','Total acumulado en 1 hora']:['1hAc','1 hour accumulate']},
				valuemax: { prms -> prms?.value1 },
				valuemin: null,
				valuemed: null
			]
		]
	]

	def readDistances(int maxDistance = 2500, int zone_id = 1) {
		def distances = []
		def strsql = "select * from dashboard.dist_zone_opoint(${maxDistance}, ${zone_id})"
		sqlconn.rows(strsql).each {row ->
			def distance = [:]
			distance['zone_abbr'] = row.zone_abbr
			distance['zone_id']   = row.zone_id
			distance['opoint_id'] = row.opoint_id
			distance['distance']  = row.distance
			distances << distance
		}
		distances
	}

	// getters
	def getOpoint = { id -> opoints.find{ it -> it?.id == id }}
	def getOpoint2 = {id -> opoints2.find{ it -> it?.id == id}}
	def getMagnitude = { id -> magnitudes.find{ it -> it?.id == id } }
	def getMagnitudeSchema = { id -> magnitude_schemas.find({ it?.magnitude_id == id }) }
	def getNasaOpoint = { id -> nasa_opoints.find{ it -> it?.id == id } }
	def getNasaMagnitude = { id -> nasa_magnitudes.find{ it -> it?.id == id } }

	/**
	 * Returns a SQL String for query all data rows between a date/time and a timw interval after or before
	 * for a specifica magnitud (no forecasting column)
	 *
	 * Each data row can be a hourly data, daily, monthly, yearly or total for the period
	 *
	 * @param itvl the interval of time
	 * @param row what type of row data:
	 * @param magnitude_id magnitude id
	 * @param opoint_id station id, null for all stations
	 * @param year the year of the date
	 * @param month the month
	 * @param dom the day of month
	 * @param hour hour of the date
	 * @return a sql query whih parameters defind
	 *
	 * @author JPSalvadorM@gmail.com
	 */
	def getSql4_1mg (itvl, row, magnitude_id, opoint_id, year, month=1, dom=1, hour = 0, forecastingIfAny = true) {
		opoint_id = (opoint_id != null)?:3

		String sqlstr  = null
		if(forecastingIfAny && [8, 10, 14, 81, 82, 83, 87, 89].contains(magnitude_id)) {
			sqlstr = """
				select
					d.magnitude_id,
					d.opoint_id,
					d.datetime,
					d.utcdatetime,
					d.magnitude_unit,
					d.utcdatetime,
					d.value1,
					d.value1x,
					d.value1y,
					d.n1,
					d.value1min,
					d.value1med,
					d.value1max,
					d.value8 value8h,
					d.value8min,
					d.value8med,
					d.value8max,
					d.value24 value24h,
					d.value24min,
					d.value24med,
					d.value24max,                
					${row == 'per hour'} isHourlyData,
					f.fc1,
					f.fc1min,
					f.fc1max,
					f.fc1sum,
					f.fc1x,
					f.fc1y,
					f.fc8,
					f.fc8min,
					f.fc8max,
					f.fc24,
					f.fc24min,
					f.fc24max
				from
					dashboard.api_dataseries_vw(
						'${itvl}',
						'${row}',
						${magnitude_id},
						${opoint_id},
						${year},
						${month},
						${dom},
						${hour}
					) d 
        			left join dashboard.api_dataseriesfc(
						'${itvl}',
						'${row}',
						${magnitude_id},
						${opoint_id},
						${year},
						${month},
						${dom},
						${hour}
					) f
					on d.magnitude_id = f.magnitude_id and d.opoint_id = f.opoint_id and d.datetime = f.datetime
				order by 1, 2, 3
			"""
		} else {
			sqlstr = """
				select
					d.magnitude_id,
					d.opoint_id,
					d.datetime,
					d.utcdatetime,
					d.magnitude_unit,
					d.utcdatetime,
					d.value1,
					d.value1x,
					d.value1y,
					d.n1,
					d.value1min,
					d.value1med,
					d.value1max,
					d.value8 value8h,
					d.value8min,
					d.value8med,
					d.value8max,
					d.value24 value24h,
					d.value24min,
					d.value24med,
					d.value24max,                
					${row == 'per hour'} isHourlyData,
					null::numeric fc1,
					null::numeric fc1min,
					null::numeric fc1max,
					null::numeric fc1sum,
					null::numeric fc1x,
					null::numeric fc1y,
					null::numeric fc8,
					null::numeric fc8min,
					null::numeric fc8max,
					null::numeric fc24,
					null::numeric fc24min,
					null::numeric fc24max
				from
					dashboard.api_dataseries_vw(
						'${itvl}',
						'${row}',
						${magnitude_id},
						${opoint_id},
						${year},
						${month},
						${dom},
						${hour}
					) d
				order by 1, 2, 3
			"""
		}
		return sqlstr
	}

	// init
    def init() {
		sqlconn = new Sql(dataSource)
		readOpoints()
		readOpoints4OpenAQ()
		readMagnitudes()
    }
}
