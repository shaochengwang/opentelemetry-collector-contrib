package metadata

import (
	"bufio"
	"fmt"
	"github.com/aws/aws-sdk-go/aws/ec2metadata"
	"github.com/aws/aws-sdk-go/aws/session"
	"io"
	"os"
)

const (
	containerIdLength = 64
	defaultCGroupPath = "/proc/self/cgroup"
)

type Metadata struct{
	ec2 *ec2metadata.EC2Metadata
	docker *DockerHelper
}

func NewMetadata(s *session.Session) *Metadata{
	return &Metadata{
		ec2: ec2metadata.New(s),
		docker: &DockerHelper{
			cGroupPath: defaultCGroupPath,
		},
	}
}
func (m *Metadata) getEC2Region() (string, error) {
	return m.ec2.Region()
}

func (m *Metadata) getEC2InstanceId() (string, error) {
	instance, err := m.ec2.GetInstanceIdentityDocument()
	if err != nil {
		return "", err
	}
	return instance.InstanceID, nil
}

func (m *Metadata) getContainerId() (string, error) {
	return m.docker.getContainerId()
}

func (m *Metadata) GetHostIdentifier() (string, error){
	id, err := m.getEC2InstanceId()
	if err == nil {
		return id, nil
	}
	//Get container ID
	id, err = m.getContainerId()
	if err == nil {
		return id, nil
	}
	return "", err
}

type DockerHelper struct{
	cGroupPath string
}

func (d *DockerHelper) getContainerId() (string, error) {
	file, err := os.Open(d.cGroupPath)
	if err != nil {
		fmt.Println("E! Read cgroup file error.")
		return "", err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	containerId := ""
	for {
		line, isPrefix, err := reader.ReadLine()
		if isPrefix {
			continue
		}
		if err != nil && err != io.EOF {
			break
		}
		if len(line) > containerIdLength {
			startIndex := len(line) - containerIdLength
			containerId = string(line[startIndex:])
			return containerId, nil
		}
		if err == io.EOF {
			break
		}
	}
	if err != io.EOF {
		return "", err
	}
	return containerId, nil
}