package daemon

import "context"

func (d *Daemon) handleResetRestoreToken(req Request) Response {
	d.mu.Lock()
	if req.Target == "" {
		response := Response{OK: false, State: d.overallStateLocked(), Error: "reset-restore-token requires a target IP"}
		d.mu.Unlock()
		return response
	}
	entry, ok := d.streams[req.Target]
	if !ok {
		response := Response{OK: false, State: d.overallStateLocked(), Error: "no active stream to " + req.Target}
		d.mu.Unlock()
		return response
	}
	if entry.state != StateStreaming {
		response := Response{OK: false, State: d.overallStateLocked(), Error: "restore token can be reset only for a fully streaming target: " + req.Target}
		d.mu.Unlock()
		return response
	}
	if entry.captureGroup == nil || d.captureGroups[entry.captureGroup.key] != entry.captureGroup {
		response := Response{OK: false, State: d.overallStateLocked(), Error: "active target has no owned capture group: " + req.Target}
		d.mu.Unlock()
		return response
	}
	for target, other := range d.streams {
		if target != req.Target && other.captureGroup == entry.captureGroup {
			response := Response{OK: false, State: d.overallStateLocked(), Error: "target uses a shared capture group; disconnect its peers before resetting the restore token"}
			d.mu.Unlock()
			return response
		}
	}
	if entry.deviceID == "" || entry.port == 0 {
		response := Response{OK: false, State: d.overallStateLocked(), Error: "active target is missing receiver identity or port: " + req.Target}
		d.mu.Unlock()
		return response
	}

	target := entry.deviceIP
	deviceID := entry.deviceID
	port := entry.port
	cleanup := d.detachStreamLocked(target)
	connCtx, cancel := context.WithCancel(context.Background())
	replacement := &activeStream{
		deviceIP:     target,
		deviceID:     deviceID,
		port:         port,
		state:        StateConnecting,
		cancelFn:     cancel,
		credentialCh: make(chan string, 1),
	}
	d.clearLastErrorForTargetLocked(target)
	d.streams[target] = replacement
	// Reserve the target and register the replacement worker before unlocking.
	// Shutdown can detach the reservation, but cannot finish Wait until this
	// reset either abandons it or hands ownership to connectAndStream.
	d.streamWorkers.Add(1)
	d.mu.Unlock()

	cleanup.run()

	d.mu.Lock()
	if d.shuttingDown || d.streams[target] != replacement {
		if d.streams[target] == replacement {
			abandoned := d.detachStreamLocked(target)
			d.mu.Unlock()
			abandoned.run()
		} else {
			d.mu.Unlock()
		}
		d.streamWorkers.Done()
		return Response{OK: false, State: StateIdle, Error: "daemon is shutting down"}
	}
	if err := d.credStore.ClearRestoreToken(deviceID); err != nil {
		abandoned := d.detachStreamLocked(target)
		state := d.overallStateLocked()
		d.mu.Unlock()
		abandoned.run()
		d.streamWorkers.Done()
		return Response{OK: false, State: state, Error: "clear restore token: " + err.Error()}
	}
	state := d.overallStateLocked()
	d.mu.Unlock()

	go func() {
		defer d.streamWorkers.Done()
		d.connectAndStream(connCtx, replacement, target, port, "")
	}()
	return Response{OK: true, State: state, Device: target, DeviceIP: target}
}
