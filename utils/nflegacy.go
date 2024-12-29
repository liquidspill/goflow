package utils

import (
	"bytes"
	"time"

	"github.com/cloudflare/goflow/v3/decoders/netflowlegacy"
	"github.com/prometheus/client_golang/prometheus"
)

type StateNFLegacy[T any] struct {
	Transport Transport[T]
	Logger    Logger

	TransformFunc   TransformFunc[T, any]
	PostProcessFunc PostProcessFunc[T]
	FlowStatsFunc   FlowStatsFunc[T]
}

func (s *StateNFLegacy[T]) DecodeFlow(msg interface{}) error {
	pkt := msg.(BaseMessage)
	buf := bytes.NewBuffer(pkt.Payload)
	key := pkt.Src.String()
	samplerAddress := pkt.Src
	if samplerAddress.To4() != nil {
		samplerAddress = samplerAddress.To4()
	}

	ts := uint64(time.Now().UTC().Unix())
	if pkt.SetTime {
		ts = uint64(pkt.RecvTime.UTC().Unix())
	}

	timeTrackStart := time.Now()
	msgDec, err := netflowlegacy.DecodeMessage(buf)

	if err != nil {
		switch err.(type) {
		case *netflowlegacy.ErrorVersion:
			NetFlowErrors.With(
				prometheus.Labels{
					"router": key,
					"error":  "error_version",
				}).
				Inc()
		}
		return err
	}

	switch msgDecConv := msgDec.(type) {
	case netflowlegacy.PacketNetFlowV5:
		NetFlowStats.With(
			prometheus.Labels{
				"router":  key,
				"version": "5",
			}).
			Inc()
		NetFlowSetStatsSum.With(
			prometheus.Labels{
				"router":  key,
				"version": "5",
				"type":    "DataFlowSet",
			}).
			Add(float64(msgDecConv.Count))
	}

	var flowMessageSet []*T
	flowMessageSet, err = s.TransformFunc(msgDec, struct{}{})

	timeTrackStop := time.Now()
	DecoderTime.With(
		prometheus.Labels{
			"name": "NetFlowV5",
		}).
		Observe(float64((timeTrackStop.Sub(timeTrackStart)).Nanoseconds()) / 1000)

	for _, fmsg := range flowMessageSet {
		s.PostProcessFunc(fmsg, PostProcessInput{
			TimeReceived:   ts,
			SamplerAddress: samplerAddress,
		})
	}

	if s.Transport != nil {
		s.Transport.Publish(flowMessageSet)
	}

	return nil
}

func (s *StateNFLegacy[T]) FlowRoutine(workers int, addr string, port int, reuseport bool) error {
	return UDPRoutine("NetFlowV5", s.DecodeFlow, workers, addr, port, reuseport, s.Logger)
}
