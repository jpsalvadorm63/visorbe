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

//	def calcDATA24h(prms) { return prms?.value24h }  //TODO: borrar this huevada

	def colorsTEMP = [
		0: 'rgb(0,191,255)',
		1: 'rgb(0,255.0)',
		2: 'rgb(255,255,0)',
		3: 'rgb(255,204,102)',
		4: 'rgb(255,0,0)',
		5: 'rgb(201,71,245)'
	]

//	const healthTEMP = (lang) => [
//			lang === 'en' ? 'low' : 'baja',
//			lang === 'en' ? 'reatively low' : 'relativamente baja',
//			lang === 'en' ? 'moderate' : 'moderada',
//			lang === 'en' ? 'confortable' : 'confortable',
//			lang === 'en' ? 'reatively high' : 'relativamente alta',
//			lang === 'en' ? 'high' : 'alta',
//	]

	def colorsHUM = [
		0: 'rgb(255,255,0)',
		1: 'rgb(255,165,0)',
		2: 'rgb(201,71,245)'
	]

//	const healthHUM = (lang) => [
//			lang === 'en' ? 'low' : 'baja',
//			lang === 'en' ? 'moderate' : 'moderada',
//			lang === 'en' ? 'high' : 'alta',
//			lang === 'en' ? 'very high' : 'muy alta',
//			lang === 'en' ? 'extremely high' : 'extremadamente alta'
//	]

	def calcIQCA24h(prms) { return (prms.isHourlyData==null || prms.isHourlyData)?prms.value24h:prms.value24max?prms.value24max:prms.value24h}

	def colorsBPressure = [
		0: 'rgb(30,144,255)',
		1: 'rgb(0,210,255)',
		2: 'rgb(252, 191, 73)',
		3: 'rgb(238,130,238)',
		4: 'rgb(138,43,226)'
	]

//	const healthBPressure = (lang) => [
//			lang === 'en' ? 'deep low presure system' : 'system baja presión profunda',
//			lang === 'en' ? 'low presure' : 'baja Presión',
//			lang === 'en' ? 'sea level presure' : 'presión al nivel del mar Estándar',
//			lang === 'en' ? 'high pressure' : 'alta Presión',
//			lang === 'en' ? 'strong high pressure system' : 'sistema de alta presión fuerte'
//	]

	def colorsRain = [
		0: 'rgba(0,228,0)',
		1: 'rgba(255,225,0)',
		2: 'rgba(255,126,0)',
		3: 'rgba(255,0,0)',
		4: 'rgba(143,63,151)'
	]

// ======= AQI =======

	def AQIidx = [0, 1, 2, 3, 4, 5, 6]

	def AQIranges = [0, 50, 100, 150, 200, 300, 400, 500]

//	def AQIhealths = [
//		0: (lang = 'en') -> (lang=='en')?'good':'bueno',
//		1: (lang = 'en') -> (lang=='en')?'moderate':'moderado',
//		2: (lang = 'en') -> (lang=='en')?'unhealthy sensitive groups':'insalubre grupos sensibles',
//		3: (lang = 'en') -> (lang=='en')?'unhealthy':'insalubre',
//		4: (lang = 'en') -> (lang=='en')?'very unhealthy':'muy insalubre',
//		5: (lang = 'en') -> (lang=='en')?'hazardous':'peligroso',
//		6: (lang = 'en') -> (lang=='en')?'very hazardous':'muy peligroso'
//	] as java.lang.Object

	def getAQIhealth(id, lang = 'en') {return AQIhealths[id]?.call(lang)}

	def AQIcolors = [
		0: 'rgb(50, 205, 50)',
		1: 'rgb(255, 255, 0)',
		2: 'rgb(255, 165, 0)',
		3: 'rgb(255, 0, 0)',
		4: 'rgb(148, 0, 211)',
		5: 'rgb(138, 0, 0)',
		6: 'rgb(128, 0, 0)'
	]

	def RADScolors = [
		0: 'rgb(0,255,0)',
		1: 'rgb(255,255,0)',
		2: 'rgb(255,165,0)',
		3: 'rgb(255,0,0)',
		5: 'rgb(201,71,245)'
	]

//	const healthRADS = (lang) => [
//		lang === 'en' ?'low':'baja',
//		lang === 'en' ?'moderate':'moderada',
//		lang === 'en' ?'high':'alta',
//		lang === 'en' ?'very high':'muy alta',
//		lang === 'en' ?'extremely high':'extremadamente alta'
//	]

	def colorsWindSpeed = [
		0: 'rgb(0,225,255)',
		1: 'rgb(0,200,255)',
		2: 'rgb(0,200,0)',
		3: 'rgb(0,225,0)',
		4: 'rgb(0,255,0)',
		5: 'rgb(255,225,0)',
		6: 'rgb(255,150,0)',
		7: 'rgb(255,0,0)',
		8: 'rgb(200,0,0)',
		9: 'rgb(150,0,0)',
		10: 'rgb(100,0,0)',
		11: 'rgb(50,0,0)',
		12: 'rgb(20,0,0)'
	]

	def colorsWindDir = [0: 'rgb(0,200,0)']

	//	const healthWindSpeed = (lang) => [ // The Beaufort Wind Force Scale: https://www.weathergamut.com/2019/02/25/weather-lingo-the-beaufort-wind-force-scale/
	//		lang === 'en' ? 'calm' : 'calma',
	//		lang === 'en' ? 'light air' : 'ventolina',
	//		lang === 'en' ? 'light breeze' : 'brisa muy débil',
	//		lang === 'en' ? 'gentle breeze' : 'brisa ligera',
	//		lang === 'en' ? 'moderate breeze' : 'brisa moderada',
	//		lang === 'en' ? 'fresh breeze' : 'brisa fresca',
	//		lang === 'en' ? 'near gale' : 'brisa fuerte',
	//		lang === 'en' ? 'gale' : 'viento fuerte',
	//		lang === 'en' ? 'strong gale': 'viento duro',
	//		lang === 'en' ? 'storm' : 'viento muy duro',
	//		lang === 'en' ? 'strong storm' : 'temporal duro',
	//		lang === 'en' ? 'very strong storm' : 'temporal muy duro',
	//		lang === 'en' ? 'hurricane' : 'temporal huracanado',
	//	]

	//const healthWindHealth = (lang) => [lang === 'en' ? 'OK' : 'OK']

	def getAQIcolors(Integer id) { AQIcolors[id] }

	// ======= IQCA =======

	int[] IQCAidx = [0, 1, 2, 3, 4, 5]

	int[] IQCAranges = [0, 50, 100, 200, 300, 400, 500]

	//	def IQCAhealths = [
	//		//idx: health
	//		0: (lang) -> (lang == 'en') ? 'Optimus': 'Óptimo',
	//		1: (lang) -> (lang == 'en') ? 'Acceptable': 'Aceptable',
	//		2: (lang) -> (lang == 'en') ? 'Caution': 'Precaución',
	//		3: (lang) -> (lang == 'en') ? 'Alert': 'Alerta',
	//		4: (lang) -> (lang == 'en') ? 'Alarm': 'Alarma',
	//		5: (lang) -> (lang == 'en') ? 'Emergency': 'Emergencia',
	//		6: (lang) -> (lang == 'en') ? 'Danger': 'Peligro'
	//	]

	def getIQCAhealth(id, lang='en') { return IQCAhealths[id]?.call(lang) }

	def IQCAcolors = AQIcolors

	def getIQCAcolor(Integer id) { IQCAcolors[id] }

	def SO2 = 1
	def PM10 = 3
	def CO = 6
	def NO = 7
	def NO2 = 8
	def PM25 = 10
	def NOX = 12
	def O3 = 14
	def RAD_SOLAR = 88
	def IUV = 99
	def Irrad_UV = 201
	def Ed305 = 93
	def Ed313 = 94
	def Ed320 = 95
	def Ed340 = 96
	def Ed380 = 97
	def Ed395 = 98
	def PAR = 100
	def RAP_VEC = 81
	def DIR_VEC = 82
	def TEMP_AMB = 83
	def HUM_REL = 86
	def PRES_BAR = 87
	def LLUVIA = 89

	def C1hranges = [
		(SO2):       [0, 62.5, 125, 200, 1000, 1800, 2600],
		(PM10):      [0, 50, 100, 250, 400, 500, 600],
		(CO):        [0, 5, 10, 15, 30, 40, 50],
		(NO):        [0, 100, 200, 1000, 2000, 3000, 4000],
		(NO2):       [0, 100, 200, 1000, 2000, 3000, 4000],
		(PM25):      [0, 25, 50, 150, 250, 350, 450],
		(NOX):       [0, 190, 381, 572, 763, 955],
		(RAD_SOLAR): [0, 375, 750, 1125, 1500, 2000],
		(IUV):       [0, 2, 5, 7, 10, 20],
		(Irrad_UV):  [0, 0.1, 0.2, 0.3, 0.4, 0.5],
		(Ed305):     [0, 4, 7, 18, 15, 20],
		(Ed313):     [0, 4, 8, 20, 30, 40],
		(Ed320):     [0, 10, 25, 35, 45, 55],
		(Ed340):     [0, 18, 37, 56, 74, 94],
		(Ed380):     [0, 24, 48, 72, 96, 125],
		(Ed395):     [0, 23, 46, 70, 93, 120],
		(PAR):       [0, 0.05, 0.10, 0.20, 0.30, 0.40],
		(RAP_VEC):   [0.0, 0.3, 1.4, 3.1, 5.3, 7.8, 10.0, 13.6, 16.9, 20.6, 24.4, 28.3,32.5, 85],
		(DIR_VEC):   [-180, 180],
		(TEMP_AMB):  [-5, 5, 10, 17, 24, 30, 40],
		(HUM_REL):   [0, 30, 70, 100],
		(PRES_BAR):  [0, 980, 1013, 1014, 1040, 2000],
		(LLUVIA):    [
			day:      [0,   5,  10,  25,   50,  100],
			week:     [0, 8.5,  17,  40,   75,  150],
			month:    [0,  20,  50, 100,  150,  250],
			quarter:  [0,  25,  50, 125,  250,  500],
			semester: [0,  50, 100, 250,  500, 1000],
			year:     [0, 100, 200, 500, 1000, 2000],
		]
	]

	def C8hranges = [
		(SO2):        null,
		(PM10):       null,
		(CO):         [0, 5, 10, 15, 30, 40, 50],
		(NO):         null,
		(NO2):        null,
		(PM25):       null,
		(NOX):        null,
		(RAD_SOLAR):  null,
		(IUV):        null,
		(Irrad_UV):   null,
		(Ed305):      null,
		(Ed313):      null,
		(Ed320):      null,
		(Ed340):      null,
		(Ed380):      null,
		(Ed395):      null,
		(PAR):        null,
		(RAP_VEC):    null,
		(DIR_VEC):   null,
		(TEMP_AMB):   null,
		(HUM_REL):    null,
		(PRES_BAR):   null,
		(LLUVIA):     null
	]

	def C24hranges = [
		(SO2):        [0, 62.5, 125, 200, 1000, 1800, 2600],
		(PM10):       [0, 50, 100, 250, 400, 500, 600],
		(CO):         null,
		(NO2):        null,
		(PM25):       [0, 25, 50, 150, 250, 350, 450],
		(NOX):        null,
		(RAD_SOLAR):  null,
		(IUV):        null,
		(Irrad_UV):   null,
		(Ed305):      null,
		(Ed313):      null,
		(Ed320):      null,
		(Ed340):      null,
		(Ed380):      null,
		(Ed395):      null,
		(PAR):        null,
		(RAP_VEC):    null,
		(DIR_VEC):    null,
		(TEMP_AMB):   null,
		(HUM_REL):    null,
		(PRES_BAR):   null,
		(LLUVIA):     null
	]

// ======= formulas =======

	def getIdxHealth(value, ranges) {
		Integer result = null
		for(int i=0; i < ranges.size-1; i++) {
			if(ranges[i] <= value && value <= ranges[i+1] && result == null) {
				result = i
				break
			}
		}
		return result
	}

	def getHealth(idx, healths, lang='en') {
		return healths[idx]?.call(lang)
	}

	// ======= schemas =======

	def magnitude_schemas = [
		[
			magnitude_id: SO2,   // magnitude_id = 1
			magnitude_name: [es:'Dióxido de Azufre [SO2]', en:'Sulfur dioxide [SO2]'],
			nasa_name: null,
			unit: 'ug/m3',
			DATA: [
				value: { prms -> return prms?.value24h },
				colDescription: {lang -> (lang == 'es')?['C24h','Concentración promedio 24 horas']:['C24h','24 hours average concentration']},
				valuemin: { prms -> prms?.value24min },
				valuemed: { prms -> prms?.value24med },
				valuemax: { prms -> prms?.value24max },
				ranges: C24hranges[SO2],                                   // (1)
				health: {value -> getIdxHealth(value, C24hranges[SO2])},   // (2)
				colors: IQCAcolors,
				decimals: 1
			],
			CONCENTRATION: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['C1h','Concentración promedio en 1 hora']:['C1h','1 hour average concentration']},
				valuemin: { prms -> prms?.value1min },
				valuemed: { prms -> prms?.value1med },
				valuemax: { prms -> prms?.value1max },
				ranges: C1hranges[1],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[SO2])},    // (4)
				colors: IQCAcolors,
				decimals: 1
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[SO2])},    // (5)
			c8h_health: null,                                            // (6)
			c24h_health: {value -> getIdxHealth(value, C24hranges[SO2])},  // (7)
			IQCA: [
				value: { prms ->
					Double iqca = null
					Double C24h = (prms.isHourlyData==null || prms.isHourlyData)?prms.value24h:prms.value24max?prms.value24max:prms.value24h
					if(iqca == null && C24h> 0 && C24h <= 62.5) iqca = Math.round(0.8*C24h)
					if(iqca == null && C24h> 62.5 && C24h <= 125) iqca = Math.round(0.8*C24h)
					if(iqca == null && C24h> 125 && C24h <= 200) iqca = Math.round((4/3)*C24h - (200/3))
					if(iqca == null && C24h> 200 && C24h <= 1000) iqca = Math.round(0.125*C24h + 175)
					if(iqca == null && C24h> 1000 && C24h <= 1800) iqca = Math.round(0.125*C24h + 175)
					if(iqca == null && C24h> 1800) iqca = Math.round(0.125*C24h + 175)
					return iqca
				},
				colDescription: { lang -> (lang == 'es') ? ['IQCA', 'Indice Quiteño de la Calidad del Aire'] : ['IQCA', 'Aire Quality Quito Index'] },
				description: { lang -> (lang == 'es') ? ['Índice Quiteño de Calidad del Aire', 'IQCA'] : ['Quito Air Quality Index', 'IQCA'] },
				ranges: IQCAranges,                                      // (8)
				health: {value -> getIdxHealth(value, IQCAranges)},      // (9)
				colors: IQCAcolors,                                      // (10)
				decimals: 0
			],
			AQI: [
				value: {
					prms ->
						def C24h = (prms.isHourlyData==null || prms.isHourlyData) ? prms.value24h : prms.value24max ? prms.value24max : prms.value24h
						Double aqi= null
						if(aqi == null && C24h> 0 && C24h <= 91.7) aqi = Math.round((50/91.7)*C24h)
						if(aqi == null && C24h> 91.7 && C24h <= 196.5) aqi = Math.round(0.4771*C24h + 6.25)
						if(aqi == null && C24h> 196.5 && C24h <= 484.7) aqi = Math.round(0.4771*C24h + 6,25)
						if(aqi == null && C24h> 484.7 && C24h <= 796.48) aqi = Math.round(0.16037*C24h + 72.26891)
						if(aqi == null && C24h> 796.48 && C24h <= 1582.48) aqi = Math.round(0.12722*C24h + 98.66667)
						if(aqi == null && C24h> 1582.48 && C24h <= 2106.48) aqi = Math.round(0.19084*C24h - 2)
						if(aqi == null && C24h> 2106.48) aqi = Math.round(0.19084*C24h - 2)
						return aqi
				},
				colDescription: { lang -> (lang == 'es') ? ['AQI', 'Indice Internacional de la Calidad del Aire'] : ['AQI', 'Aire Quality Index'] },
				description: { lang -> (lang == 'es') ? ['Índice Internacional de Calidad del Aire', 'AQI'] : ['Air Quality Index', 'AQI'] },
				ranges: AQIranges,                                       // (11)
				health: {value -> getIdxHealth(value, AQIranges)},       // (12)
				colors: AQIcolors,                                       // (13)
				decimals: 0
			],
		],
		[
			magnitude_id: PM10,    // magnitude_id = 3
			magnitude_name: [es:'Material Particulado 10 [PM10]', en:'Particulate matter 10 [PM10]'],
			nasa_name: null,
			unit: 'ug/m3',
			DATA: [
				value: { prms -> prms?.value24h },
				colDescription: {lang -> (lang == 'es')?['C24h','Concentración promedio 24 horas']:['C24h','24 hours average concentration']},
				valuemin: { prms -> prms?.value24min },
				valuemed: { prms -> prms?.value24med },
				valuemax: { prms -> prms?.value24max },
				ranges: C24hranges[PM10],                                 // (1)
				health: {value -> getIdxHealth(value, C24hranges[PM10])}, // (2)
				colors: IQCAcolors,                                       // 2.1
				decimals: 1                                               // 2.2
			],
			CONCENTRATION: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['C1h','Concentración promedio en 1 hora']:['C1h','1 hour average concentration']},
				valuemin: { prms -> prms?.value1min },
				valuemed: { prms -> prms?.value1med },
				valuemax: { prms -> prms?.value1max },
				ranges: C1hranges[PM10],                                   // (3)
				health: {value -> getIdxHealth(value, C1hranges[PM10])},   // (4)
				colors: IQCAcolors,                                        // 4.1
				decimals: 1                                                // 4.2
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[PM10])},   // (5)
			c8h_health: null,                                              // (6)
			c24h_health: {value -> getIdxHealth(value, C24hranges[PM10])}, // (7)
			IQCA: [
				value: { prms ->
					Double C24h = (prms.isHourlyData==null || prms.isHourlyData)?prms.value24h:prms.value24max?prms.value24max:prms.value24h
					Double iqca = null
					if(iqca == null && C24h> 0 && C24h <= 50) iqca = Math.round(C24h)
					if(iqca == null && C24h> 50 && C24h <= 100) iqca = Math.round(C24h)
					if(iqca == null && C24h> 100 && C24h <= 250) iqca = Math.round((2/3.0)*C24h+(100/3.0))
					if(iqca == null && C24h> 250 && C24h <= 400) iqca = Math.round((2/3.0)*C24h+(100/3.0))
					if(iqca == null && C24h> 400 && C24h <= 500) iqca = Math.round(C24h-100)
					if(iqca == null && C24h> 500) iqca = Math.round(C24h-100)
					return iqca
				},
				colDescription: { lang -> (lang == 'es') ? ['IQCA', 'Indice Quiteño de la Calidad del Aire'] : ['IQCA', 'Aire Quality Quito Index'] },
				description: { lang -> (lang == 'es' ? ['Índice Quiteño de Calidad del Aire', 'IQCA'] : ['Quito Air Quality Index', 'IQCA']) },
				ranges: IQCAranges,                                      // (8)
				health: {value -> getIdxHealth(value, IQCAranges)},      // (9)
				colors: IQCAcolors,                                      // (10)
				decimals: 1
			],
			AQI: [
				value: {prms ->
					Double C24h = (prms.isHourlyData==null || prms.isHourlyData)?prms.value24h:prms.value24max?prms.value24max:prms.value24h
					Double aqi= null
					if(aqi == null && C24h> 0 && C24h <= 54) aqi = Math.round((25/27.0)*C24h)
					if(aqi == null && C24h> 54 && C24h <= 154) aqi = Math.round(0.5*C24h + 23)
					if(aqi == null && C24h> 154 && C24h <= 254) aqi = Math.round(0.5*C24h + 23)
					if(aqi == null && C24h> 254 && C24h <= 354) aqi = Math.round(0.5*C24h + 23)
					if(aqi == null && C24h> 354 && C24h <= 424) aqi = Math.round(1.42857*C24h - 305.71428)
					if(aqi == null && C24h> 424 && C24h <= 504) aqi = Math.round(1.25*C24h - 230)
					if(aqi == null && C24h> 504) aqi = Math.round(1*C24h - 104)
					return aqi
				},
				colDescription: { lang -> (lang == 'es') ? ['AQI', 'Indice Internacional de la Calidad del Aire'] : ['AQI', 'Aire Quality Index'] },
				description: { lang -> (lang == 'es' ? ['Índice Internacional de Calidad del Aire', 'AQI'] : ['Air Quality Index', 'AQI']) },
				ranges: AQIranges,                                       // (11)
				health: {value -> getIdxHealth(value, AQIranges)},       // (12)
				colors: AQIcolors,                                       // (13)
				decimals: 1
			]
		],
		[
			magnitude_id: CO,    // magnitude_id = 6
			magnitude_name: [es:'Monóxido de Carbono [CO]', en:'Carbon monoxide [CO]'],
			nasa_name: null,
			unit: 'ug/m3',
			DATA: [
				value: { prms -> prms?.value8h },
				colDescription: {lang -> (lang == 'es')?['C8h','Concentración promedio en 8 horas']:['C8h','8 hours average concentration']},
				valuemin: { prms -> prms?.value8min },
				valuemed: { prms -> prms?.value8med },
				valuemax: { prms -> prms?.value8max },
				ranges: C8hranges[CO],                                   // (1)
				health: {value -> getIdxHealth(value, C8hranges[CO])},   // (2)
				colors: IQCAcolors,                                      // 2.1
				decimals: 1                                              // 2.2
			],
			CONCENTRATION: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['C1h','Concentración promedio en 1 hora']:['C1h','1 hour average concentration']},
				valuemin: { prms -> prms?.value1min },
				valuemed: { prms -> prms?.value1med },
				valuemax: { prms -> prms?.value1max },
				ranges: C1hranges[CO],                                   // (3)
				health: {value -> getIdxHealth(value, C1hranges[CO])},   // (4)
				colors: IQCAcolors,                                      // 4.1
				decimals: 1                                              // 4.2
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[CO])},   // (5)
			c8h_health: {value -> getIdxHealth(value, C8hranges[CO])},   // (6)
			c24h_health:  null,                                          // (7)
			IQCA: [
				value: { prms ->
					Double C8h = (prms.isHourlyData==null || prms.isHourlyData) ? prms.value8h : prms.value8max ? prms.value8max : prms.value8h
					Double iqca = null
					if(iqca == null && C8h> 0 && C8h <= 5) iqca = Math.round(C8h*10)
					if(iqca == null && C8h> 5 && C8h <= 10) iqca = Math.round(C8h*10)
					if(iqca == null && C8h> 10 && C8h <= 15) iqca = Math.round(C8h*20 - 100)
					if(iqca == null && C8h> 15 && C8h <= 30) iqca = Math.round((2/3.0)*C8h+100)
					if(iqca == null && C8h> 30 && C8h <= 40) iqca = Math.round(C8h*10)
					if(iqca == null && C8h> 40) iqca = Math.round(C8h*10)
					return iqca
				},
				colDescription: { lang -> (lang == 'es') ? ['IQCA', 'Indice Quiteño de la Calidad del Aire'] : ['IQCA', 'Aire Quality Quito Index'] },
				description: { lang -> (lang == 'es' ? ['Índice Quiteño de Calidad del Aire', 'IQCA'] : ['Quito Air Quality Index', 'IQCA']) },
				ranges: IQCAranges,                                      // (8)
				health: {value -> getIdxHealth(value, IQCAranges)},      // (9)
				colors: IQCAcolors,                                      // (10)
				decimals: 0
			],
			AQI: [
				value: {prms ->
					def C8h = (prms.isHourlyData==null || prms.isHourlyData)?prms.value8h:prms.value8max?prms.value8max:prms.value8h
					Double aqi= null
					if(aqi == null && C8h> 0 && C8h <= 5.038) aqi = Math.round((50/5.038)*C8h)
					if(aqi == null && C8h> 5.038 && C8h <= 10.763) aqi = Math.round(8.73362*C8h + 6)
					if(aqi == null && C8h> 10.763 && C8h <= 14.198) aqi = Math.round(14.55605*C8h - 56.66667)
					if(aqi == null && C8h> 14.198 && C8h <= 17.633) aqi = Math.round(14.55605*C8h - 56.66667)
					if(aqi == null && C8h> 17.633 && C8h <= 34.808) aqi = Math.round(5.82241*C8h + 97.3333)
					if(aqi == null && C8h> 34.808 && C8h <= 46.258) aqi = Math.round(8.73362*C8h - 4)
					if(aqi == null && C8h> 46.258) aqi = Math.round(8.73362*C8h - 4)
					return aqi
				},
				colDescription: { lang -> (lang == 'es') ? ['AQI', 'Indice Internacional de la Calidad del Aire'] : ['AQI', 'Aire Quality Index'] },
				description: { lang -> (lang == 'es' ? ['Índice Internacional de Calidad del Aire', 'AQI'] : ['Air Quality Index', 'AQI']) },
				ranges: AQIranges,                                       // (11)
				health: {value -> getIdxHealth(value, AQIranges)},       // (12)
				colors: AQIcolors,                                       // (13)
				decimals: 0
			]
	    ],
		[
			magnitude_id: NO2,   // id:8
			magnitude_name: [es:'Dióxido de Nitrógeno [NO2]', en:'Nitrogen dioxide [NO2]'],
			nasa_name: 'no2_ML_[ugm-3]',
			unit: 'ug/m3',
			DATA: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['C1h','Concentración promedio en 1 hora']:['C1h','1 hour average concentration']},
				valuemin: { prms -> prms?.value1min },
				valuemed: { prms -> prms?.value1med },
				valuemax: { prms -> prms?.value1max },
				ranges: C1hranges[NO2],                                  // (1)
				health: {value -> getIdxHealth(value, C1hranges[NO2])},  // (2)
				colors: IQCAcolors,                                      // 2.1
				decimals: 1                                              // 2.2
			],
			DATA2: null,
			CONCENTRATION: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['C1h','Concentración promedio en 1 hora']:['C1h','1 hour average concentration']},
				valuemin: { prms -> prms?.value1min },
				valuemed: { prms -> prms?.value1med },
				valuemax: { prms -> prms?.value1max },
				ranges: C1hranges[NO2],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[NO2])},    // (4)
				colors: IQCAcolors,                                        // 4.1
				decimals: 1                                                // 4.2
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[NO2])},    // (5)
			c8h_health: null,                                              // (6)
			c24h_health: null,                                             // (7)
			IQCA: [
				value: {prms ->
					Double C1h = prms.value1
					Double iqca = null
					if(iqca == null && C1h> 0 && C1h <= 100) iqca = Math.round(C1h*0.5)
					if(iqca == null && C1h> 100 && C1h <= 200) iqca = Math.round(C1h*0.5)
					if(iqca == null && C1h> 200 && C1h <= 1000) iqca = Math.round(0.125*C1h+75)
					if(iqca == null && C1h> 1000 && C1h <= 2000) iqca = Math.round(0.1*C1h+100)
					if(iqca == null && C1h> 2000 && C1h <= 3000) iqca = Math.round(0.1*C1h+100)
					if(iqca == null && C1h> 3000) iqca = Math.round(0.1*C1h+100)
					return iqca
				},
				colDescription: { lang -> (lang == 'es') ? ['IQCA', 'Indice Quiteño de la Calidad del Aire'] : ['IQCA', 'Aire Quality Quito Index'] },
				description: { lang -> (lang == 'es' ? ['Índice Quiteño de Calidad del Aire', 'IQCA'] : ['Quito Air Quality Index', 'IQCA']) },
				ranges: IQCAranges,                                      // (8)
				health: {value -> getIdxHealth(value, IQCAranges)},      // (9)
				colors: IQCAcolors,                                      // (10)
				decimals: 0
			],
			AQI: [
				value: {prms ->
					Double C1h = prms.value1
					Double aqi= null
					if(aqi == null && C1h> 0 && C1h <= 99.64) aqi = Math.round(0.50181*C1h)
					if(aqi == null && C1h> 99.64 && C1h <= 188) aqi = Math.round(0.56587*C1h - 6.38298)
					if(aqi == null && C1h> 188 && C1h <= 676.8) aqi = Math.round(0.10229*C1h + 80.76923)
					if(aqi == null && C1h> 676.8 && C1h <= 1220.12) aqi = Math.round(0.09202*C1h + 87.71626)
					if(aqi == null && C1h> 1220.12 && C1h <= 2348.12) aqi = Math.round(0.08865*C1h + 91.83333)
					if(aqi == null && C1h> 2348.12 && C1h <= 3100.12) aqi = Math.round(0.13298*C1h - 12.25)
					if(aqi == null && C1h> 3100.12) aqi = Math.round(0.13298*C1h - 12.25)
					return aqi
				},
				colDescription: { lang -> (lang == 'es') ? ['AQI', 'Indice Internacional de la Calidad del Aire'] : ['AQI', 'Aire Quality Index'] },
				description: { lang -> (lang == 'es' ? ['Índice Internacional de Calidad del Aire', 'AQI'] : ['Air Quality Index', 'AQI']) },
				ranges: AQIranges,                                       // (11)
				health: {value -> getIdxHealth(value, AQIranges)},       // (12)
				colors: AQIcolors,                                       // (13)
				decimals: 0
			]
		],
		[
			magnitude_id: PM25,     // magnitude_id = 10
			magnitude_name: [es:'Material Particulado 2.5 [PM2.5]', en:'Particulate matter [PM2.5]'],
			nasa_name: 'pm25_gcc_ML_[ugm-3]',
			unit: 'ug/m3',
			DATA: [
				value: { prms -> prms?.value24h },
				colDescription: {lang -> (lang == 'es')?['C24h','Concentración promedio últimas 24 horas']:['C24h','last 24 hours average concentration']},
				valuemin: {prms -> prms?.value24min},
				valuemed: { prms -> prms?.value24med },
				valuemax: { prms -> prms?.value24max },
				ranges: C24hranges[10],                                  // (1)
				health: {value -> getIdxHealth(value, C24hranges[10])},  // (2)
				colors: IQCAcolors,                                      // 2.1
				decimals: 1                                              // 2.2
			],
			CONCENTRATION: [
				value: { prms -> prms.value1 },
				colDescription: {lang -> (lang == 'es')?['C1h','Concentración promedio en 1 hora']:['C1h','1 hour average concentration']},
				valuemin: { prms -> prms?.value1min },
				valuemed: { prms -> prms?.value1med },
				valuemax: { prms -> prms?.value1max },
				ranges: C1hranges[PM25],                                   // (3)
				health: {value -> getIdxHealth(value, C1hranges[PM25])},   // (4)
				colors: IQCAcolors,                                        // 4.1
				decimals: 1                                                // 4.2
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[PM25])},   // (5)
			c8h_health: null,                                              // (6)
			c24h_health: {value -> getIdxHealth(value, C24hranges[PM25])}, // (7)
			IQCA: [
				value: { prms ->
					Double C24h = (prms.isHourlyData==null || prms.isHourlyData)?prms.value24h:prms.value24max?prms.value24max:prms.value24h
					Double iqca = null
					if(iqca == null && C24h> 0 && C24h <= 25) iqca = Math.round(2*C24h)
					if(iqca == null && C24h> 25 && C24h <= 50) iqca = Math.round(2*C24h)
					if(iqca == null && C24h> 50 && C24h <= 150) iqca = Math.round(C24h+50)
					if(iqca == null && C24h> 150 && C24h <= 250) iqca = Math.round(C24h+50)
					if(iqca == null && C24h> 250 && C24h <= 350) iqca = Math.round(C24h+50)
					if(iqca == null && C24h> 350) iqca = Math.round(C24h+50)
					return iqca
				},
				colDescription: { lang -> (lang == 'es') ? ['IQCA', 'Indice Quiteño de la Calidad del Aire'] : ['IQCA', 'Aire Quality Quito Index'] },
				description: { lang -> (lang == 'es') ? ['Índice Quiteño de Calidad del Aire', 'IQCA'] : ['Quito Air Quality Index', 'IQCA'] },
				ranges: IQCAranges,                                      // (8)
				health: {value -> getIdxHealth(value, IQCAranges)},      // (9)
				colors: IQCAcolors,                                      // (10)
				decimals: 0
			],
			AQI: [
				value: { prms ->
					def C24h = (prms.isHourlyData==null || prms.isHourlyData)?prms.value24h:prms.value24max?prms.value24max:prms.value24h
					Double aqi= null
					if(aqi == null && C24h> 0 && C24h <= 12) aqi = Math.round((25/6.0)*C24h)
					if(aqi == null && C24h> 12 && C24h <= 35.4) aqi = Math.round(2.13675*C24h + 24.35897)
					if(aqi == null && C24h> 35.4 && C24h <= 55.4) aqi = Math.round(2.5*C24h + 11.5)
					if(aqi == null && C24h> 55.4 && C24h <= 150.4) aqi = Math.round(0.52631*C24h + 120.8421)
					if(aqi == null && C24h> 150.4 && C24h <= 250.4) aqi = Math.round(1*C24h + 49.6)
					if(aqi == null && C24h> 250.4 && C24h <= 350.4) aqi = Math.round(1*C24h + 49.6)
					if(aqi == null && C24h> 350.4) aqi = Math.round(1*C24h + 49.6)
					return aqi
				},
				colDescription: { lang -> (lang == 'es') ? ['AQI', 'Indice Internacional de la Calidad del Aire'] : ['AQI', 'Aire Quality Index'] },
				description: { lang -> (lang == 'es' ? ['Índice Internacional de Calidad del Aire', 'AQI'] : ['Air Quality Index', 'AQI']) },
				ranges: AQIranges,                                       // (11)
				health: {value -> getIdxHealth(value, AQIranges)},       // (12)
				colors: AQIcolors,                                       // (13)
				decimals: 1
			]
		],
		[
			magnitude_id: O3,                  // magnitude_id = 14
			magnitude_name: [es:'Ozono [O3]', en:'Ozone [O3]'],
			nasa_name: 'o3_ML_[ugm-3]',
			unit: 'ug/m3',
			DATA: [
				value: {prms -> prms?.value8h},
				colDescription: {lang -> (lang == 'es')?['C8h','Concentración promedio en 8 horas']:['C8h','8 hours average concentration']},
				valuemin: { prms -> prms?.value1min },
				valuemed: { prms -> prms?.value1med },
				valuemax: { prms -> prms?.value1max },
				ranges: C1hranges[O3],                                   // (3)
				health: { prms ->
					Double C1h = prms?.value1
					Double C8h = prms?.value8h
					if(value8h> 0 && value <= 50) return 0
					else if(value8h> 50 && value <= 100) return 1
					else if(value> 100 && value <= 200) return 2
					else if(value> 200 && value <= 400) return 3
					else if(value> 408 && value <= 808) return 3
					else if(value> 808 && value <= 1008) return 4
					else if(value> 1008 /*&& value <= 1200*/) return 5
					else return null
				},                                                       // (4)
				colors: IQCAcolors,                                      // 4.1
				decimals: 1                                              // 4.2
			],
			CONCENTRATION: null,
			c1h_health: {value ->
				if(value> 408 && value <= 808) return 3 else
				if(value> 808 && value <= 1008) return 4 else
				if(value> 1008 /*&& value <= 1200*/) return 5 else return null
			},                                                           // (5)
			c8h_health: {value ->
				if(value> 0 && value <= 50) return 0 else
				if(value> 50 && value <= 100) return 1 else
				if(value> 100 && value <= 200) return 2 else
				if(value> 200 && value <= 400) return 3 else return null
			},                                                           // (6)
			c24h_health: null,                                           // (7)
			IQCA: [
				value: {prms ->
					Double C1h = (prms.isHourlyData==null || prms.isHourlyData)?prms.value1:prms.value1max?prms.value1max:prms.value1
					Double C8h = (prms.isHourlyData==null || prms.isHourlyData)?prms.value8h:prms.value8max?prms.value8max:prms.value8h
					Double iqca = null
					if(iqca == null && C8h> 0 && C8h <= 50) iqca = Math.round(C8h)
					if(iqca == null && C8h> 50 && C8h <= 100) iqca = Math.round(C8h)
					if(iqca == null && C8h> 100 && C8h <= 200) iqca = Math.round(C8h)
					if(iqca == null && C8h> 200 && C8h <= 400) iqca = Math.round(0.5*C8h+100)
					if(iqca == null && C1h> 408 && C1h <= 808) iqca = Math.round(0.25*C1h + 98)
					if(iqca == null && C1h> 808 && C1h <= 1008) iqca = Math.round(0.5*C1h -104)
					if(iqca == null && C1h> 1008 && C1h <= 1200) iqca = Math.round((100/192.0)*C1h -125)
					return iqca
				},
				colDescription: { lang -> (lang == 'es') ? ['IQCA', 'Indice Quiteño de la Calidad del Aire'] : ['IQCA', 'Aire Quality Quito Index'] },
				description: { lang -> (lang == 'es' ? ['Índice Quiteño de Calidad del Aire', 'IQCA'] : ['Quito Air Quality Index', 'IQCA']) },
				ranges: IQCAranges,                                      // (8)
				health: {value -> getIdxHealth(value, IQCAranges)},      // (9)
				colors: IQCAcolors,                                      // (10)
				decimals: 0
			],
			AQI: [
				value: {prms ->
					Double C8h = (prms.isHourlyData==null || prms.isHourlyData)?prms.value8h:prms.value8max?prms.value8max:prms.value8h
					Double C1h = (prms.isHourlyData==null || prms.isHourlyData)?prms.value1:prms.value1max?prms.value1max:prms.value1
					Double aqi= null
					if(aqi == null && C8h > 0 && C8h <= 108) aqi = Math.round((50/108)*C8h)
					if(aqi == null && C8h > 108 && C8h <= 140) aqi = Math.round(1,5625*C8h-118,75)
					if(aqi == null && C8h > 140 && C8h <= 170) aqi = Math.round(1,667*C8h-133,333)
					if(aqi == null && C8h > 170 && C8h <= 210) aqi = Math.round(1,25*C8h-62,5)
					if(aqi == null && C8h > 210 && C8h <= 400) aqi = Math.round(0.526316*C8h+89.47368)
					if(aqi == null && C1h > 250 && C1h <= 328) aqi = Math.round(0,641025*C1h -60,25641)
					if(aqi == null && C1h > 328 && C1h <= 408) aqi = Math.round(0,625*C1h - 55)
					if(aqi == null && C1h > 408 && C1h <= 808) aqi = Math.round(0,25*C1h + 98)
					if(aqi == null && C1h > 808 && C1h <= 1008) aqi = Math.round(0,5*C1h - 104)
					if(aqi == null && C1h > 1008) aqi = Math.round(0,5*C1h - 104)
					return aqi
				},
				colDescription: { lang -> (lang == 'es') ? ['AQI', 'Indice Internacional de la Calidad del Aire'] : ['AQI', 'Aire Quality Index'] },
				description: { lang -> (lang == 'es' ? ['Índice Internacional de Calidad del Aire', 'AQI'] : ['Air Quality Index', 'AQI']) },
				ranges: AQIranges,                                       // (11)
				health: {value -> getIdxHealth(value, AQIranges)},       // (12)
				colors: AQIcolors,                                       // (13)
				decimals: 1
			]
		],
		[
			magnitude_id: NO,
			magnitude_name: [es:'Monóxido de Nitrógeno [NO]', en:'Nitrogen monoxide [NO]'],
			nasa_name: null,
			unit: 'ug/m3',
			DATA: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['C1h','Concentración promedio en 1 hora']:['C1h','1 hour average concentration']},
				valuemin: { prms -> prms?.value1min },
				valuemed: { prms -> prms?.value1med },
				valuemax: { prms -> prms?.value1max },
				ranges: C1hranges[NO],                                   // (1)
				health: {value ->
					println "hola ..... $value"
					return getIdxHealth(value, C1hranges[NO])
				},   // (2)
				colors: IQCAcolors,
				decimals: 1
			],
			CONCENTRATION: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['C1h','Concentración promedio en 1 hora']:['C1h','1 hour average concentration']},
				valuemin: { prms -> prms?.value1min },
				valuemed: { prms -> prms?.value1med },
				valuemax: { prms -> prms?.value1max },
				ranges: C1hranges[NO],                                   // (1)
				health: {value -> getIdxHealth(value, C1hranges[NO])},   // (2)
				colors: IQCAcolors,
				decimals: 1
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[NO])},   // (5)
			c8h_health: null,  // (6)
			c24h_health: null, // (7)
		],
		[
			magnitude_id: NOX,   // 12,
			magnitude_name: [es:'Óxido de Nitrógeno [NOx]', en:'Nitrogen oxide [NOx]'],
			unit: 'ug/m3',
			DATA: [
				value: {prms -> prms?.value1},
				colDescription: {lang -> (lang == 'es')?['1hC','Concentración promedio en 1 hora']:['1hC','1 hour average concentration']},
				valuemin: {prms -> prms?.value1min},
				valuemed: { prms -> prms?.value1med },
				valuemax: {prms -> prms?.value1max},
				ranges: C1hranges[NOX],                                   // (1)
				health: {value -> getIdxHealth(value, C1hranges[NOX])},   // (2)
				colors: IQCAcolors,
				decimals: 1
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1},
				colDescription: {lang -> (lang == 'es')?['1hC','Concentración promedio en 1 hora']:['1hC','1 hour average concentration']},
				valuemin: {prms -> prms?.value1min},
				valuemed: { prms -> prms?.value1med },
				valuemax: {prms -> prms?.value1max},
				ranges: C1hranges[NOX],                                   // (1)
				health: {value -> getIdxHealth(value, C1hranges[NOX])},   // (2)
				colors: IQCAcolors,
				decimals: 1
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[NO])},   // (5)
			c8h_health: null,  // (6)
			c24h_health: null, // (7)
		],
		[
			magnitude_id: RAD_SOLAR,
			magnitude_name: [es:'Radiación Solar', en:'Solar radiation'],
			nasa_name: null,
			unit: 'W/m2',
			DATA: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C24hranges[RAD_SOLAR],                                   // (1)
				health: {value -> getIdxHealth(value, C24hranges[RAD_SOLAR])},   // (2)
				colors: RADScolors,
				decimals: 1
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C24hranges[RAD_SOLAR],                                   // (1)
				health: {value -> getIdxHealth(value, C24hranges[RAD_SOLAR])},   // (2)
				colors: RADScolors,
				decimals: 1
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[RAD_SOLAR])},    // (5)
			c8h_health: null,   // (6)
			c24h_health: null,  // (7)
		],
		[
			magnitude_id: IUV,
			magnitude_name: [es:'Indice Radiación UV', en:'UV radiation index [IUV]'],
			unit: '',
			DATA: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C1hranges[IUV],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[IUV])},    // (4)
				colors: RADScolors,
				decimals: 1
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C1hranges[IUV],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[IUV])},    // (4)
				colors: RADScolors,
				decimals: 1
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[IUV])},    // (5)
			c8h_health: null,   // (6)
			c24h_health: null,  // (7)
		],
		[
			magnitude_id: Irrad_UV,
			magnitude_name: [es: 'Irradiancia UV', en:'UV Radiance'],
			nasa_name: null,
			unit: 'W/m2',
			DATA: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C1hranges[Irrad_UV],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[Irrad_UV])},    // (4)
				colors: RADScolors,
				decimals: 1
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C1hranges[Irrad_UV],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[Irrad_UV])},    // (4)
				colors: RADScolors,
				decimals: 1
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[Irrad_UV])},    // (5)
			c8h_health: null,   // (6)
			c24h_health: null,  // (7)
		],
		[
			magnitude_id: Ed305,
			magnitude_name: [es:'Ed305', en:'Ed305'],
			unit: 'uW/(cm2nm)',
			DATA: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C1hranges[Ed305],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[Ed305])},    // (4)
				colors: RADScolors,
				decimals: 1
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C1hranges[Ed305],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[Ed305])},    // (4)
				colors: RADScolors,
				decimals: 1
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[Ed305])},    // (5)
			c8h_health: null,   // (6)
			c24h_health: null,  // (7)
		],
		[
			magnitude_id: Ed313,
			magnitude_name: [es:'Ed313', en:'Ed313'],
			nasa_name: null,
			unit: 'uW/(cm2nm)',
			DATA: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C1hranges[Ed313],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[Ed313])},    // (4)
				colors: RADScolors,
				decimals: 1
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C1hranges[Ed313],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[Ed313])},    // (4)
				colors: RADScolors,
				decimals: 1
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[Ed313])},    // (5)
			c8h_health: null,   // (6)
			c24h_health: null,  // (7)
		],
		[
			magnitude_id: Ed320,
			magnitude_name: [es:'Ed320', en:'Ed320'],
			unit: 'uW/(cm2nm)',
			DATA: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C1hranges[Ed320],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[Ed320])},    // (4)
				colors: RADScolors,
				decimals: 1
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C1hranges[Ed320],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[Ed320])},    // (4)
				colors: RADScolors,
				decimals: 1
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[Ed320])},    // (5)
			c8h_health: null,   // (6)
			c24h_health: null,  // (7)
		],
		[
			magnitude_id: Ed340,
			magnitude_name: [es:'Ed340', en:'Ed340'],
			nasa_name: null,
			unit: 'uW/(cm2nm)',
			DATA: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C1hranges[Ed340],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[Ed340])},    // (4)
				colors: RADScolors,
				decimals: 1
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C1hranges[Ed340],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[Ed340])},    // (4)
				colors: RADScolors,
				decimals: 1
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[Ed340])},    // (5)
			c8h_health: null,   // (6)
			c24h_health: null,  // (7)
		],
		[
			magnitude_id: Ed380,
			magnitude_name: [es:'Ed380', en:'Ed380'],
			nasa_name: null,
			unit: 'uW/(cm2nm)',
			DATA: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C1hranges[Ed380],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[Ed380])},    // (4)
				colors: RADScolors,
				decimals: 1
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C1hranges[Ed380],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[Ed380])},    // (4)
				colors: RADScolors,
				decimals: 1
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[Ed380])},    // (5)
			c8h_health: null,   // (6)
			c24h_health: null,  // (7)
		],
		[
			magnitude_id: Ed395,
			magnitude_name: [es:'Ed395', en:'Ed395'],
			nasa_name: null,
			unit: 'uW/(cm2nm)',
			DATA: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C1hranges[Ed395],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[Ed395])},    // (4)
				colors: RADScolors,
				decimals: 1
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C1hranges[Ed395],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[Ed395])},    // (4)
				colors: RADScolors,
				decimals: 1
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[Ed395])},    // (5)
			c8h_health: null,   // (6)
			c24h_health: null,  // (7)
		],
		[
			magnitude_id: PAR,
			magnitude_name: [es:'PAR', en:'PAR'],
			nasa_name: null,
			unit: 'uE/(cm2seg)',
			DATA: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C1hranges[PAR],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[PAR])},    // (4)
				colors: RADScolors,
				decimals: 1
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1max},
				colDescription: {lang -> (lang == 'es')?['10mX','Máximo promedio en 10 minuntes']:['10mX','Max average in 10 minutes']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C1hranges[PAR],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[PAR])},    // (4)
				colors: RADScolors,
				decimals: 1
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[PAR])},    // (5)
			c8h_health: null,   // (6)
			c24h_health: null,  // (7)
		],
		[
			magnitude_id: RAP_VEC,
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
				valuemed: null,
				ranges: C1hranges[RAP_VEC],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[RAP_VEC])},    // (4)
				colors: colorsWindSpeed,
				decimals: 1
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1},
				valuex: {prms -> prms?.value1x},
				valuey: {prms -> prms?.value1y},
				colDescription: {lang -> (lang == 'es')?['1hP','promedio 1 hora']:['1hP','1 hour average']},
				valuemax: {prms -> prms?.value1max},
				valuemin: null,
				valuemed: null,
				ranges: C1hranges[RAP_VEC],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[RAP_VEC])},    // (4)
				colors: colorsWindSpeed,
				decimals: 1
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[RAP_VEC])},    // (5)
			c8h_health: null,   // (6)
			c24h_health: null,  // (7)
		],
		[
			magnitude_id: DIR_VEC,
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
				valuemed: null,
				ranges: C1hranges[DIR_VEC],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[DIR_VEC])},    // (4)
				colors: colorsWindDir,
				decimals: 1
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1},
				valuex: {prms -> prms?.value1x},
				valuey: {prms -> prms?.value1y},
				colDescription: {lang -> (lang == 'es')?['1hP','promedio 1 hora']:['1hP','1 hour average']},
				valuemax: null,
				valuemin: null,
				valuemed: null,
				ranges: C1hranges[DIR_VEC],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[DIR_VEC])},    // (4)
				colors: colorsWindDir,
				decimals: 1
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[DIR_VEC])},    // (5)
			c8h_health: null,   // (6)
			c24h_health: null,  // (7)
		],
		[
			magnitude_id: TEMP_AMB,
			magnitude_name: [es:'Temperatura Media', en:'Average temperature'],
			nasa_name: 'Temperature_[K]',
			unit: 'oC',
			DATA: [
				value: {prms -> prms?.value1},
				colDescription: {lang -> (lang == 'es')?['1hP','promedio 1 hora']:['1hP','1 hour average']},
				valuemin: {prms -> prms?.value1min},
				valuemed: { prms -> prms?.value1med },
				valuemax: {prms -> prms?.value1max},
				ranges: C1hranges[TEMP_AMB],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[TEMP_AMB])},    // (4)
				colors: colorsTEMP,
				decimals: 1
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1},
				colDescription: {lang -> (lang == 'es')?['1hP','promedio 1 hora']:['1hP','1 hour average']},
				valuemin: {prms -> prms?.value1min},
				valuemed: { prms -> prms?.value1med },
				valuemax: {prms -> prms?.value1max},
				ranges: C1hranges[TEMP_AMB],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[TEMP_AMB])},    // (4)
				colors: colorsTEMP,
				decimals: 1
			]
		],
		[
			magnitude_id: HUM_REL,
			magnitude_name: [es:'Humedad relativa', en:'Relative humidity'],
			nasa_name: null,
			unit: '%',
			DATA: [
				value: {prms -> prms?.value1},
				colDescription: {lang -> (lang == 'es')?['1hP','promedio 1 hora']:['1hP','1 hour average']},
				valuemin: {prms -> prms?.value1min},
				valuemed: { prms -> prms?.value1med },
				valuemax: {prms -> prms?.value1max},
				ranges: C1hranges[HUM_REL],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[HUM_REL])},    // (4)
				colors: colorsHUM,
				decimals: 1
			],
			CONCENTRATION: [
				value: {prms -> prms?.value1},
				colDescription: {lang -> (lang == 'es')?['1hP','promedio 1 hora']:['1hP','1 hour average']},
				valuemin: {prms -> prms?.value1min},
				valuemed: { prms -> prms?.value1med },
				valuemax: {prms -> prms?.value1max},
				ranges: C1hranges[HUM_REL],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[HUM_REL])},    // (4)
				colors: colorsHUM,
				decimals: 1
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[HUM_REL])},    // (5)
			c8h_health: null,   // (6)
			c24h_health: null,  // (7)
		],
		[
			magnitude_id: PRES_BAR,
			magnitude_name: [es:'Presión barométrica', en:'Barometric Pressure'],
			nasa_name: 'SurfacePressure_[hPa]',
			unit: 'mb',
			DATA: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['1hP','promedio 1 hora']:['1hP','1 hour average']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C1hranges[PRES_BAR],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[PRES_BAR])},    // (4)
				colors: colorsBPressure,
				decimals: 1
			],
			CONCENTRATION: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['1hP','promedio 1 hora']:['1hP','1 hour average']},
				valuemin: null,
				valuemed: null,
				valuemax: null,
				ranges: C1hranges[PRES_BAR],                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[PRES_BAR])},    // (4)
				colors: colorsBPressure,
				decimals: 1
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[PRES_BAR])},    // (5)
			c8h_health: null,   // (6)
			c24h_health: null,  // (7)
		],
		[
			magnitude_id: LLUVIA,
			magnitude_name: [es:'Lluvia acumulada', en:'Accumulate rainfall'],
			nasa_name: 'Precipitation_[mm]',
			unit: 'mm',
			DATA: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['1hAc','Total acumulado en 1 hora']:['1hAc','1 hour accumulate']},
				valuemax: { prms -> prms?.value1 },
				valuemin: null,
				valuemed: null,
				ranges: C1hranges[LLUVIA],                                        // (3)
				health: {value -> getIdxHealth(value, C1hranges[LLUVIA].day)},    // (4)
				colors: colorsRain,
				decimals: 1
			],
			CONCENTRATION: [
				value: { prms -> prms?.value1 },
				colDescription: {lang -> (lang == 'es')?['1hAc','Total acumulado en 1 hora']:['1hAc','1 hour accumulate']},
				valuemax: { prms -> prms?.value1 },
				valuemin: null,
				valuemed: null,
				ranges: C1hranges[LLUVIA].day,                                    // (3)
				health: {value -> getIdxHealth(value, C1hranges[LLUVIA].day)},    // (4)
				colors: colorsRain,
				decimals: 1
			],
			c1h_health: {value -> getIdxHealth(value, C1hranges[LLUVIA].day)},    // (5)
			c8h_health: null,   // (6)
			c24h_health: null,  // (7)
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
	 * @param magnitudes csv of magnitudes or null for all magnitudes
	 * @param opoints csv of stations or null for all stations
	 * @param year the year of the date
	 * @param month the month
	 * @param dom the day of month
	 * @param hour hour of the date
	 * @return a sql query whih parameters defind
	 *
	 * @author JPSalvadorM@gmail.com
	 */
	def getSql4_1mg (itvl, row, magnitude_id, opoints, year, month=1, dom=1, hour = 0, forecastingIfAny = true) {
		return """
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
				f.fc24max,
				d.iqca,
				d.aqi
			from
				dashboard.api_dataseries_vw(
					'${itvl}',
					'${row}',
					${magnitude_id},
					null,
					${year},
					${month},
					${dom},
					${hour}
				) d 
				left join dashboard.api_dataseriesfc(
					'${itvl}',
					'${row}',
					${magnitude_id},
					null,
					${year},
					${month},
					${dom},
					${hour}
				) f
				on d.magnitude_id = f.magnitude_id and d.opoint_id = f.opoint_id and d.datetime = f.datetime
				where d.opoint_id in (${opoints})
			order by 1, 2, 3
		"""
	}

	// init
    def init() {
		sqlconn = new Sql(dataSource)
		readOpoints()
		readOpoints4OpenAQ()
		readMagnitudes()
    }
}
