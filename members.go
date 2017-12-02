package dmutex

import (
	"strconv"
	"strings"

	"github.com/bparli/dmutex/quorums"
	"github.com/hashicorp/memberlist"
	log "github.com/sirupsen/logrus"
)

var memberlistConfig *memberlist.Config

func InitMembersList(localAddr string, peers []string) (*quorums.MemList, error) {
	memberlistConfig = memberlist.DefaultLANConfig()
	memberlistConfig.Events = newEventDelegate()
	addr := strings.Split(localAddr, ":")
	memberlistConfig.AdvertiseAddr = addr[0]
	memberlistConfig.BindAddr = addr[0]
	memberlistConfig.Name = addr[0]

	// if port is not given use the default memberlist port (7946)
	if len(addr) == 2 {
		if memberPort, err := strconv.Atoi(addr[1]); err == nil {
			memberlistConfig.AdvertisePort = memberPort
			memberlistConfig.BindPort = memberPort
		}
	}

	mlist, err := memberlist.Create(memberlistConfig)
	if err != nil {
		return nil, err
	}

	_, err = mlist.Join(peers)
	if err != nil {
		log.Errorln("Failed to join cluster: ", err.Error())
	}
	return &quorums.MemList{mlist}, nil
}
