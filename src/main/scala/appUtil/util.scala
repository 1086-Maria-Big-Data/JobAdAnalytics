package appUtil

import java.util.Properties
import org.apache.log4j.PropertyConfigurator

object Util {

    private val path = "/dev/generic.properties"

    def loadConfig(): Properties = {

        val props = new Properties
        props.load(getClass().getResourceAsStream(path))

        PropertyConfigurator.configure(props)

        return props
    }
}