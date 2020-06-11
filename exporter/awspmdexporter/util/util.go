package util

import (
	"io/ioutil"
"log"
"os"
"path/filepath"
"strings"
)

const cwagentVersionFileName = "CWAGENT_VERSION"

// We will fall back to a major version if no valid version file is found
const defaultVersion = "1"

var version string

func Version() string {
	if version != "" {
		return version
	}
	version = defaultVersion
	ex, err := os.Executable()
	if err != nil {
		log.Printf("W! Cannot get the path for current executable binary: %v", err)
		return version
	}
	curPath := filepath.Dir(ex)
	versionFilePath := filepath.Join(curPath, cwagentVersionFileName)
	if _, err := os.Stat(versionFilePath); err != nil {
		log.Printf("W! The agent Version file %s does not exist: %v", versionFilePath, err)
		return version
	}

	byteArray, err := ioutil.ReadFile(versionFilePath)
	if err != nil {
		log.Printf("W! Issue encountered when reading content from file %s: %v", versionFilePath, err)
		return version
	}

	//TODO we may consider to do a format checking for the Version value.
	version = strings.Trim(string(byteArray), " \n\r\t")
	log.Printf("I! AmazonCloudWatchAgent Version %s.", version)
	return version
}