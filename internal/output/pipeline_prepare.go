package output

// validatePipelinePreparation runs before reservation/native commit. Publication
// receives the identical digest-bound metadata, so capacity conversion cannot
// discover a larger body/member demand after the native transaction commits.
func validatePipelinePreparation(b PipelineBinding, plan PipelinePlan, outputs []PipelineOutput) error {
	if len(outputs) > 10000 || PipelinePreparedSHA256(outputs) != b.PreparedSHA256 {
		return ErrPipelineConflict
	}
	remaining := b.Demand
	remaining.ArtifactBytes -= b.PlanRef.Bytes + b.ManifestBytesLimit
	if len(outputs) > 0 && b.ManifestBytesLimit <= 0 {
		return ErrPipelineCapacity
	}
	remaining.MetadataRows -= int64(3 + len(plan.Targets))
	remaining.ProofSlots = 0
	if !remaining.valid() {
		return ErrPipelineCapacity
	}
	var references int64
	ids := map[string]bool{}
	targets := map[string]PipelineTarget{}
	for _, t := range plan.Targets {
		targets[t.ID] = t
	}
	for _, o := range outputs {
		t, ok := targets[o.PipelineID]
		if !ok || o.Incarnation != t.Incarnation || o.RuleRevision != t.RuleRevision || o.DestinationRevision != t.DestinationRevision || !pipelineID(o.ID) || ids[o.ID] || !pipelineID(o.Generation) || !o.Body.valid() || !(o.Kind == "delivery" || o.Kind == "fragment") || len(o.Members) == 0 || len(o.Members) > 100 || !o.Charge.valid() || o.Charge.ProofSlots != 0 || o.Charge.References != int64(len(o.Members)) || o.Charge.MetadataRows < int64(1+len(o.Members)) || o.Charge.ArtifactBytes < o.Body.Bytes || o.Charge.QueuedBytes < o.Body.Bytes {
			return ErrPipelineConflict
		}
		ids[o.ID] = true
		members := map[string]bool{}
		var encoded int64
		for _, m := range o.Members {
			if !pipelineID(m.UUID) || members[m.UUID] || m.Bytes <= 0 || m.Bytes > o.Body.Bytes-encoded {
				return ErrPipelineConflict
			}
			members[m.UUID] = true
			encoded += m.Bytes
		}
		if o.Kind == "fragment" && encoded > o.Charge.QueuedBytes-int64(len(o.Members))*1024 {
			return ErrPipelineCapacity
		}
		if !o.Charge.fits(remaining) {
			return ErrPipelineCapacity
		}
		remaining.References -= o.Charge.References
		remaining.QueuedBytes -= o.Charge.QueuedBytes
		remaining.MetadataRows -= o.Charge.MetadataRows
		remaining.ArtifactBytes -= o.Charge.ArtifactBytes
		references += o.Charge.References
	}
	if references != b.Demand.References {
		return ErrPipelineConflict
	}
	return nil
}
