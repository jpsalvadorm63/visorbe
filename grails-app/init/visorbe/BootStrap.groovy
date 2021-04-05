package visorbe

class BootStrap {
	def modelService

    def init = { servletContext ->
		modelService.init()
    }

    def destroy = {
    }
}
