package visorbe

class UrlMappings {
    static mappings = {
		get "/dshbrd/csvdn"(controller:'dataseries', action:'qryDsCSV')
		get "/dshbrd/jsondn"(controller:'dataseries', action:'qryDsJSON')
		get "/dshbrd/mimapa"(controller:'dataseries', action:'mimapa')
		get "/analytics/fc"(controller:'analytics', action:'analysisForecasting')
		get "/analytics/ld"(controller:'analytics', action:'analysisLastData')
		get "/analytics/tot"(controller:'analytics', action:'analysisTot')
		get "/analytics/totmagn"(controller:'analytics', action:'analysisTotMagnitude')
		get "/analytics/totmagndate"(controller:'analytics', action:'analysisTotMagnitudeDate') // http://localhost:9090/analytics/totmagndate?itvl=1+month&magn=8&opoint=5&year=2020&month=3&dom=13
		get "/ws/dbresult"(controller:'ws', action:'qwerydb')
		get "/dshbrd/dataseries"(controller:'dashboard', action:'qryDataseriesVw')
		get "/dshbrd/dataseriesfc"(controller:'dashboard', action:'qryDataseriesFc')
		get "/dshbrd/dataseriesdn"(controller:'dashboard', action:'qryDataseriesDn')
		get "/dshbrd/distances"(controller:'dashboard', action:'qryDistances')
		get "/dshbrd/dashboard"(controller:'dashboard', action:'qryDataseriesDashboard')
		get "/openaq/location"(controller:'openaq', action:'qryOpoint')
		get "/openaq/magnitudes"(controller:'openaq', action:'qryAQMagnitudes')
		get "/openaq/dataset"(controller:'openaq', action:'qryDataset')
		"/"(view:'/notFound')
        "500"(view:'/error')
        "404"(view:'/notFound')
    }
}
