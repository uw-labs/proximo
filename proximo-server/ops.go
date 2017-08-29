package main

import (
	"github.com/utilitywarehouse/go-operational/op"
)

func getOpStatus(statuser statuser) *op.Status {
	st := op.NewStatus("Proximo", "Serving ").
		AddChecker("queue check", func(cr *op.CheckResponse) {
			ok, errs := statuser.Status()
			if !ok {
				var s string
				for _, err := range errs {
					s += err.Error()
				}
				cr.Unhealthy(s, "Check if desired connection is available for handler", "Proximo can't run")
				return
			}
			cr.Healthy("Server is healthy")
		}).
		ReadyUseHealthCheck().
		SetRevision(gitHash).
		AddOwner(appMeta.owner, appMeta.slack)

	for _, l := range appMeta.links {
		st.AddLink(l.description, l.url)
	}

	return st
}
