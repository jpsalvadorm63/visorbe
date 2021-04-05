package visorbe

class UrlMappings {
    static mappings = {
		get "/ws/dbresult"(controller:'ws', action:'qwerydb')
		get "/dshbrd/dataseries"(controller:'dashboard', action:'qryDataseriesVw')
		get "/dshbrd/dataseriesfc"(controller:'dashboard', action:'qryDataseriesFc')
		get "/dshbrd/dataseriesdn"(controller:'dashboard', action:'qryDataseriesDn')
		get "/dshbrd/csvdn"(controller:'dataseries', action:'qryDsCSV')
		get "/dshbrd/distances"(controller:'dashboard', action:'qryDistances')
		get "/dshbrd/dashboard"(controller:'dashboard', action:'qryDataseriesDashboard')
		get "/openaq/location"(controller:'openaq', action:'qryOpoint')
		get "/openaq/magnitudes"(controller:'openaq', action:'qryAQMagnitudes')
		get "/openaq/dataset"(controller:'openaq', action:'qryDataset')
		"/"(view: '/notFound')
        "500"(view: '/error')
        "404"(view: '/notFound')
    }
}
