package appUtil

import scala.io.Source
import java.util.Properties

object Util {

    def loadConfig(): Map[String, String] = {
        return Source
            .fromFile("app.config")
            .getLines()
            .map(_.split("=").map(x => x.trim))
            .map(token => (token(0), token(1)))
            .toMap
    }
}