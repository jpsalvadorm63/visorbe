package visorbe

import grails.config.Config
import grails.core.support.GrailsConfigurationAware

import static org.springframework.http.HttpStatus.OK


class DashboardService implements GrailsConfigurationAware {
	static scope = "prototype"
	String csvMimeType
	String encoding
	static responseFormats = ['json', 'xml']

	@Override
	void setConfiguration(Config co) {
		csvMimeType = co.getProperty('grails.mime.types.csv', String, 'text/csv')
		encoding = co.getProperty('grails.converters.encoding', String, 'UTF-8')
	}

	//---

	def dataSource

    def p89(params) {  // rain
		def outs = response.outputStream

    }
}
