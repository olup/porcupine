package output

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"github.com/olup/porcupine/internal/delivery"
)

type PipelineFragmentSpec struct {
	WindowStartMS, WindowEndMS int64
	Rule                       delivery.WebhookRule
}

func (s PipelineFragmentSpec) valid() bool {
	interval := s.Rule.IntervalSeconds * 1000
	return s.Rule.DeliveryMode == "batch" && s.Rule.IntervalSeconds >= 60 && s.Rule.IntervalSeconds <= 86400 &&
		s.WindowStartMS >= 0 && s.WindowStartMS%interval == 0 && s.WindowEndMS > s.WindowStartMS &&
		s.WindowEndMS-s.WindowStartMS == interval && len(s.Rule.IncludeEvents) <= 100 && len(s.Rule.ExcludeEvents) <= 100
}

type PipelineFragment struct {
	Claim             PipelineClaim
	Target            PipelineTarget
	Spec              PipelineFragmentSpec
	Body              PipelineRef
	Sequence          int64
	Next              int
	Count             int
	Members           []PipelineMember
	Recompose, Frozen bool
}

const pipelineFragmentFields = `j.source_id,j.project_id,j.catalog_id,j.stream_id,j.tenant_id,j.id,j.incarnation,j.generation,j.version,
 j.pipeline_id,j.rule_revision,j.destination_revision,j.creator_id,j.body_key,j.body_version,j.body_sha,j.body_bytes,j.sequence,j.next_member,j.refs,j.fragment_spec,j.needs_recompose,j.send_cut IS NOT NULL`

func scanPipelineFragment(row pipelineRunScanner) (PipelineFragment, error) {
	var f PipelineFragment
	c, t, r := &f.Claim, &f.Target, &f.Body
	var spec string
	err := row.Scan(&c.Scope.SourceID, &c.Scope.ProjectID, &c.Scope.CatalogID, &c.Scope.StreamID, &c.TenantID, &c.ID, &c.Incarnation, &c.Generation, &c.Version,
		&t.ID, &t.RuleRevision, &t.DestinationRevision, &t.CreatorID, &r.Key, &r.VersionID, &r.SHA256, &r.Bytes, &f.Sequence, &f.Next, &f.Count, &spec, &f.Recompose, &f.Frozen)
	t.Incarnation = c.Incarnation
	if err != nil {
		return f, err
	}
	if json.Unmarshal([]byte(spec), &f.Spec) != nil || !f.Spec.valid() || !r.valid() || f.Next < 0 || f.Next >= f.Count || f.Count > delivery.WebhookChunkEvents {
		return f, ErrPipelineConflict
	}
	return f, nil
}

func (p *PGPipelineEffects) NextFragment(ctx context.Context, db *sql.DB, source string) (PipelineFragment, error) {
	return scanPipelineFragment(db.QueryRowContext(ctx, `SELECT `+pipelineFragmentFields+` FROM pipeline_effect_jobs j
 JOIN pipeline_effect_definitions p ON
 (p.project_id,p.tenant_id,p.id,p.incarnation,p.destination_revision,p.creator_id)=
 (j.project_id,j.tenant_id,j.pipeline_id,j.incarnation,j.destination_revision,j.creator_id)
 WHERE j.source_id=$1 AND ((j.kind='fragment' AND j.state='pending') OR (j.kind='delivery' AND j.state='retired' AND j.needs_recompose)) AND j.fragment_spec<>'' AND p.state='active'
 AND j.window_end_ms<=extract(epoch FROM clock_timestamp())*1000 AND j.next_attempt_at<=clock_timestamp()
 AND NOT EXISTS (SELECT 1 FROM project_deletions x WHERE x.project_id=j.project_id)
 AND NOT EXISTS (SELECT 1 FROM tenant_deletions x WHERE x.tenant_id=j.tenant_id)
 ORDER BY j.window_end_ms,j.project_id,j.sequence,j.id LIMIT 1`, source))
}

// AssemblyFragments reads a bounded, stable window prefix. The coordinator
// session is required while the caller verifies deletion and freezes the input.
func (p *PGPipelineEffects) AssemblyFragments(ctx context.Context, db *sql.DB, first PipelineFragment, maxRead int64) ([]PipelineFragment, error) {
	c, t := first.Claim, first.Target
	if first.Recompose {
		f, err := scanPipelineFragment(db.QueryRowContext(ctx, `SELECT `+pipelineFragmentFields+` FROM pipeline_effect_jobs j WHERE `+pipelineScopeWhere+`
 AND tenant_id=$5 AND id=$6 AND kind='delivery' AND state='retired' AND needs_recompose`, append(pipelineScopeArgs(c.Scope), c.TenantID, c.ID)...))
		if err != nil {
			return nil, err
		}
		f.Members, err = p.DeliveryMembers(ctx, db, c)
		if err != nil {
			return nil, err
		}
		return []PipelineFragment{f}, nil
	}
	rows, err := db.QueryContext(ctx, `SELECT `+pipelineFragmentFields+` FROM pipeline_effect_jobs j WHERE `+pipelineScopeWhere+`
 AND j.tenant_id=$5 AND j.pipeline_id=$6 AND j.incarnation=$7 AND j.rule_revision=$8 AND j.destination_revision=$9 AND j.creator_id=$10
 AND j.fragment_spec<>'' AND j.kind='fragment' AND j.state='pending' AND j.window_start_ms=$11 AND j.window_end_ms=$12
 ORDER BY j.sequence,j.id LIMIT 101`, append(pipelineScopeArgs(c.Scope), c.TenantID, t.ID, t.Incarnation, t.RuleRevision, t.DestinationRevision, t.CreatorID, first.Spec.WindowStartMS, first.Spec.WindowEndMS)...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var fragments []PipelineFragment
	var members int
	var metadataBytes int
	for rows.Next() {
		f, err := scanPipelineFragment(rows)
		if err != nil {
			return nil, err
		}
		spec, err := json.Marshal(f.Spec)
		if err != nil {
			return nil, err
		}
		cost := len(spec) + 6000 + f.Count*250
		if f.Body.Bytes > maxRead || members+f.Count > 150 || metadataBytes+cost > 500000 {
			break
		}
		fragments = append(fragments, f)
		maxRead -= f.Body.Bytes
		members += f.Count
		metadataBytes += cost
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	if len(fragments) == 0 {
		return nil, ErrPipelineConflict
	}
	for i := range fragments {
		f := &fragments[i]
		f.Members, err = p.DeliveryMembers(ctx, db, f.Claim)
		if err != nil {
			return nil, err
		}
		if len(f.Members) != f.Count || pipelineDigest(f.Spec) != pipelineDigest(first.Spec) {
			return nil, ErrPipelineConflict
		}
	}
	return fragments, nil
}

type PipelineAssemblyInput struct {
	Fragment PipelineFragment
	Eligible []int
}

type PipelineAssembly struct {
	Scope                                 PipelineScope
	TenantID, Generation, Attempt, Prefix string
	Cut, ArtifactLimit                    int64
	Inputs                                []PipelineAssemblyInput
}

func (a PipelineAssembly) valid() bool {
	if !a.Scope.valid() || !pipelineID(a.TenantID) || !pipelineID(a.Attempt) || !pipelineID(a.Generation) || len(a.Prefix) == 0 || len(a.Prefix) > 2048 ||
		a.Cut <= 0 || a.ArtifactLimit < 1 || a.ArtifactLimit > 16<<20 || len(a.Inputs) == 0 || len(a.Inputs) > 101 {
		return false
	}
	seen := map[string]bool{}
	first := a.Inputs[0].Fragment
	if first.Recompose && len(a.Inputs) != 1 {
		return false
	}
	for _, in := range a.Inputs {
		f := in.Fragment
		c := f.Claim
		if c.Scope != a.Scope || c.TenantID != a.TenantID || !pipelineID(c.ID) || seen[c.ID] || f.Sequence <= 0 || f.Sequence > a.Cut || f.Target != first.Target || f.Recompose != first.Recompose ||
			!f.Body.valid() || len(f.Members) != f.Count || f.Count > 100 || f.Next < 0 || f.Next >= f.Count || pipelineDigest(f.Spec) != pipelineDigest(first.Spec) {
			return false
		}
		seen[c.ID] = true
		for i, index := range in.Eligible {
			if index < f.Next || index >= f.Count || i > 0 && index <= in.Eligible[i-1] {
				return false
			}
		}
	}
	return first.Spec.valid()
}

func (p *PGPipelineEffects) Assembly(ctx context.Context, db *sql.DB, s PipelineScope, tenant string) (PipelineAssembly, error) {
	var raw string
	err := db.QueryRowContext(ctx, `SELECT snapshot FROM pipeline_effect_assemblies WHERE `+pipelineScopeWhere+` AND tenant_id=$5`, append(pipelineScopeArgs(s), tenant)...).Scan(&raw)
	if err != nil {
		return PipelineAssembly{}, err
	}
	var a PipelineAssembly
	if err = json.Unmarshal([]byte(raw), &a); err != nil {
		return a, err
	}
	if a.Scope != s || a.TenantID != tenant || !a.valid() {
		return a, ErrPipelineConflict
	}
	return a, nil
}

// ReserveAssemblyTx freezes the exact membership and owns the isolated S3
// prefix before an upload. A successor can replay the same content address.
func (p *PGPipelineEffects) ReserveAssemblyTx(ctx context.Context, tx *sql.Tx, a PipelineAssembly) error {
	if tx == nil || !a.valid() {
		return ErrPipelineConflict
	}
	raw, err := json.Marshal(a)
	if err != nil {
		return err
	}
	if len(raw) > 524288 {
		return ErrPipelineCapacity
	}
	// One claim, one cleanup owner, one unsent delivery and at most 100 memberships.
	d := PipelineDemand{MetadataRows: 103, ArtifactBytes: a.ArtifactLimit}
	b := PipelineBinding{Scope: a.Scope, TenantID: a.TenantID, Demand: d}
	if err := p.lockCapacity(ctx, tx, b, true); err != nil {
		return err
	}
	seq, _, err := p.lockStream(ctx, tx, b)
	if err != nil {
		return err
	}
	if seq != a.Cut {
		return ErrPipelineConflict
	}
	if err := p.verifyAssemblyInputs(ctx, tx, a); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO pipeline_effect_assemblies (source_id,project_id,catalog_id,stream_id,tenant_id,generation,attempt,cut,snapshot)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, append(pipelineScopeArgs(a.Scope), a.TenantID, a.Generation, a.Attempt, a.Cut, string(raw))...)
	if err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO pipeline_effect_cleanup (source_id,project_id,catalog_id,stream_id,tenant_id,owner_id,generation,prefix,refs,bytes,rows,artifacts)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,0,0,$9,$10)`, append(pipelineScopeArgs(a.Scope), a.TenantID, a.Generation, a.Attempt, a.Prefix, d.MetadataRows, d.ArtifactBytes)...)
	if err != nil {
		return err
	}
	return p.charge(ctx, tx, b, d, 1)
}

func (p *PGPipelineEffects) verifyAssemblyInputs(ctx context.Context, tx *sql.Tx, a PipelineAssembly) error {
	for _, in := range a.Inputs {
		f := in.Fragment
		c := f.Claim
		got, err := scanPipelineFragment(tx.QueryRowContext(ctx, `SELECT `+pipelineFragmentFields+` FROM pipeline_effect_jobs j WHERE `+pipelineScopeWhere+`
 AND j.tenant_id=$5 AND j.id=$6 AND ((NOT $7 AND j.kind='fragment' AND j.state='pending') OR ($7 AND j.kind='delivery' AND j.state='retired' AND j.needs_recompose)) FOR UPDATE`, append(pipelineScopeArgs(a.Scope), a.TenantID, c.ID, f.Recompose)...))
		if err != nil {
			return err
		}
		copy := f
		copy.Members = nil
		if pipelineDigest(got) != pipelineDigest(copy) {
			return ErrPipelineConflict
		}
		rows, err := tx.QueryContext(ctx, `SELECT uuid,encoded_bytes FROM pipeline_effect_members WHERE `+pipelineScopeWhere+`
 AND job_id=$5 ORDER BY position LIMIT 101`, append(pipelineScopeArgs(a.Scope), c.ID)...)
		if err != nil {
			return err
		}
		var members []PipelineMember
		for rows.Next() {
			var m PipelineMember
			if err = rows.Scan(&m.UUID, &m.Bytes); err != nil {
				break
			}
			members = append(members, m)
		}
		if err := errors.Join(err, rows.Err(), rows.Close()); err != nil {
			return err
		}
		if !slices.Equal(members, f.Members) {
			return ErrPipelineConflict
		}
	}
	t := a.Inputs[0].Fragment.Target
	var configured string
	err := tx.QueryRowContext(ctx, `SELECT state FROM pipeline_effect_definitions WHERE project_id=$1 AND tenant_id=$2 AND id=$3 FOR UPDATE`,
		a.Scope.ProjectID, a.TenantID, t.ID).Scan(&configured)
	if err != nil {
		return err
	}
	if configured != "active" {
		return ErrPipelineAuthority
	}
	state, err := p.currentTarget(ctx, tx, PipelineBinding{Scope: a.Scope, TenantID: a.TenantID}, t)
	if err != nil {
		return err
	}
	if state == "retired" {
		return ErrPipelineAuthority
	}
	return nil
}

// CheckAssemblyTx revalidates a replay before staging against its retained
// source membership and current creator/definition.
func (p *PGPipelineEffects) CheckAssemblyTx(ctx context.Context, tx *sql.Tx, a PipelineAssembly) error {
	if tx == nil || !a.valid() {
		return ErrPipelineConflict
	}
	b := PipelineBinding{Scope: a.Scope, TenantID: a.TenantID}
	if err := p.lockCapacity(ctx, tx, b, false); err != nil {
		return err
	}
	if _, _, err := p.lockStream(ctx, tx, b); err != nil {
		return err
	}
	return p.verifyAssemblyInputs(ctx, tx, a)
}

type PipelineAssemblyResult struct {
	ID             string
	Body, Manifest PipelineRef
	Count          int
}

func (p *PGPipelineEffects) CompleteAssemblyTx(ctx context.Context, tx *sql.Tx, a PipelineAssembly, r PipelineAssemblyResult) error {
	if tx == nil || !a.valid() || r.Count < 0 || r.Count > 100 || !r.validObjects(a) {
		return ErrPipelineConflict
	}
	b := PipelineBinding{Scope: a.Scope, TenantID: a.TenantID}
	if err := p.lockCapacity(ctx, tx, b, false); err != nil {
		return err
	}
	seq, _, err := p.lockStream(ctx, tx, b)
	if err != nil {
		return err
	}
	if seq < a.Cut {
		return ErrPipelineConflict
	}
	raw, err := json.Marshal(a)
	if err != nil {
		return err
	}
	var frozen string
	err = tx.QueryRowContext(ctx, `SELECT snapshot FROM pipeline_effect_assemblies WHERE `+pipelineScopeWhere+` AND tenant_id=$5
 AND generation=$6 AND attempt=$7 FOR UPDATE`, append(pipelineScopeArgs(a.Scope), a.TenantID, a.Generation, a.Attempt)...).Scan(&frozen)
	if err != nil {
		return err
	}
	if frozen != string(raw) {
		return ErrPipelineConflict
	}
	if err := p.verifyAssemblyInputs(ctx, tx, a); err != nil {
		return err
	}
	var members []PipelineMember
	var bytes int64
	for _, in := range a.Inputs {
		f := in.Fragment
		count := min(r.Count-len(members), len(in.Eligible))
		next := f.Count
		if count < len(in.Eligible) {
			next = in.Eligible[count]
		}
		var transferred int64
		for _, ix := range in.Eligible[:count] {
			m := f.Members[ix]
			members = append(members, m)
			transferred += m.Bytes + 1024
		}
		bytes += transferred
		res, err := tx.ExecContext(ctx, `UPDATE pipeline_effect_jobs SET next_member=$7,version=version+1,
 state=CASE WHEN needs_recompose THEN 'retired' WHEN $7=refs THEN 'succeeded' ELSE 'pending' END,needs_recompose=false,transferred_refs=transferred_refs+$8,transferred_bytes=transferred_bytes+$9
 WHERE `+pipelineScopeWhere+` AND id=$5 AND version=$6`, append(pipelineScopeArgs(a.Scope), f.Claim.ID, f.Claim.Version, next, count, transferred)...)
		if err != nil {
			return err
		}
		if err = pipelineOne(res); err != nil {
			return err
		}
	}
	if len(members) != r.Count || r.Body.Bytes > bytes || a.Inputs[0].Fragment.Recompose && len(a.Inputs[0].Eligible) != r.Count {
		return ErrPipelineConflict
	}
	if len(members) > 0 {
		f := a.Inputs[0].Fragment
		t := f.Target
		var created sql.NullTime
		var version int64
		if f.Recompose {
			if (r.ID == f.Claim.ID) == f.Frozen {
				return ErrPipelineConflict
			}
			if !f.Frozen {
				var err error
				created, err = p.rehomeUnfrozenAssemblyTx(ctx, tx, a, f)
				version = f.Claim.Version + 1
				if err != nil {
					return err
				}
			}
		}
		spec, err := json.Marshal(f.Spec)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO pipeline_effect_jobs(source_id,project_id,catalog_id,stream_id,tenant_id,id,batch_id,pipeline_id,incarnation,rule_revision,destination_revision,creator_id,generation,kind,sequence,body_key,body_version,body_sha,body_bytes,state,refs,bytes,rows,artifacts,window_start_ms,window_end_ms,fragment_spec,assembly_manifest_key,assembly_manifest_version,assembly_manifest_sha,assembly_manifest_bytes,created_at,version)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$7,'delivery',$13,$14,$15,$16,$17,'pending',$18,$19,$20,$17,$21,$22,$23,$24,$25,$26,$27,coalesce($28,clock_timestamp()),$29)`,
			append(pipelineScopeArgs(a.Scope), a.TenantID, r.ID, a.Generation, t.ID, t.Incarnation, t.RuleRevision, t.DestinationRevision, t.CreatorID, a.Cut, r.Body.Key, r.Body.VersionID, r.Body.SHA256, r.Body.Bytes, len(members), bytes, len(members)+1, f.Spec.WindowStartMS, f.Spec.WindowEndMS, string(spec), r.Manifest.Key, r.Manifest.VersionID, r.Manifest.SHA256, r.Manifest.Bytes, created, version)...)
		if err != nil {
			return err
		}
		for pos, m := range members {
			_, err = tx.ExecContext(ctx, `INSERT INTO pipeline_effect_members(source_id,project_id,catalog_id,stream_id,job_id,position,uuid,encoded_bytes) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
				append(pipelineScopeArgs(a.Scope), r.ID, pos, m.UUID, m.Bytes)...)
			if err != nil {
				return err
			}
		}
	}

	outputRows := 0
	if len(members) > 0 {
		outputRows = len(members) + 1
	}
	res, err := tx.ExecContext(ctx, `UPDATE pipeline_effect_cleanup SET rows=1,artifacts=$8 WHERE `+pipelineScopeWhere+`
 AND tenant_id=$5 AND owner_id=$6 AND generation=$7`, append(pipelineScopeArgs(a.Scope), a.TenantID, a.Generation, a.Attempt, r.Manifest.Bytes)...)
	if err != nil {
		return err
	}
	if err = pipelineOne(res); err != nil {
		return err
	}
	unused := PipelineDemand{MetadataRows: int64(102 - outputRows), ArtifactBytes: a.ArtifactLimit - r.Body.Bytes - r.Manifest.Bytes}
	if err = p.charge(ctx, tx, b, unused, -1); err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `DELETE FROM pipeline_effect_assemblies WHERE `+pipelineScopeWhere+` AND generation=$5`, append(pipelineScopeArgs(a.Scope), a.Generation)...)
	return err
}

// Keep the existing public ID until its first freeze, while the old immutable
// bytes and their exact ownership remain attached to the original prefix.
func (p *PGPipelineEffects) rehomeUnfrozenAssemblyTx(ctx context.Context, tx *sql.Tx, a PipelineAssembly, f PipelineFragment) (sql.NullTime, error) {
	var created sql.NullTime
	var owner string
	var d PipelineDemand
	err := tx.QueryRowContext(ctx, `DELETE FROM pipeline_effect_jobs WHERE `+pipelineScopeWhere+`
 AND tenant_id=$5 AND id=$6 AND version=$7 AND state='retired' AND send_cut IS NULL
 RETURNING batch_id,created_at,refs-transferred_refs,bytes-transferred_bytes,rows,artifacts`,
		append(pipelineScopeArgs(a.Scope), a.TenantID, f.Claim.ID, f.Claim.Version+1)...).
		Scan(&owner, &created, &d.References, &d.QueuedBytes, &d.MetadataRows, &d.ArtifactBytes)
	if err != nil {
		return created, err
	}
	res, err := tx.ExecContext(ctx, `UPDATE pipeline_effect_cleanup SET refs=refs+$7,bytes=bytes+$8,rows=rows+$9,artifacts=artifacts+$10
 WHERE `+pipelineScopeWhere+` AND tenant_id=$5 AND owner_id=$6`,
		append(pipelineScopeArgs(a.Scope), a.TenantID, owner, d.References, d.QueuedBytes, d.MetadataRows, d.ArtifactBytes)...)
	if err != nil {
		return created, err
	}
	return created, pipelineOne(res)
}

func (r PipelineAssemblyResult) validObjects(a PipelineAssembly) bool {
	if r.Count == 0 {
		return r.ID == "" && r.Body == (PipelineRef{}) && r.Manifest == (PipelineRef{})
	}
	return pipelineID(r.ID) && r.Body.valid() && r.Manifest.valid() && strings.HasPrefix(r.Body.Key, a.Prefix) &&
		strings.HasPrefix(r.Manifest.Key, a.Prefix) && r.Body.Bytes <= a.ArtifactLimit-r.Manifest.Bytes
}

// AbandonAssemblyTx discards a frozen membership invalidated by deletion.
// Ownership of every possible staged object remains in cleanup.
func (p *PGPipelineEffects) AbandonAssemblyTx(ctx context.Context, tx *sql.Tx, a PipelineAssembly) error {
	if tx == nil || !a.valid() {
		return ErrPipelineConflict
	}
	b := PipelineBinding{Scope: a.Scope, TenantID: a.TenantID}
	if err := p.lockCapacity(ctx, tx, b, false); err != nil {
		return err
	}
	if _, _, err := p.lockStream(ctx, tx, b); err != nil {
		return err
	}
	args := append(pipelineScopeArgs(a.Scope), a.TenantID, a.Generation, a.Attempt)
	result, err := tx.ExecContext(ctx, `DELETE FROM pipeline_effect_assemblies WHERE `+pipelineScopeWhere+` AND tenant_id=$5 AND generation=$6 AND attempt=$7`, args...)
	if err != nil {
		return err
	}
	if err = pipelineOne(result); err != nil {
		return err
	}
	result, err = tx.ExecContext(ctx, `UPDATE pipeline_effect_cleanup SET rows=rows-1 WHERE `+pipelineScopeWhere+` AND tenant_id=$5 AND owner_id=$6 AND generation=$7`, args...)
	if err != nil {
		return err
	}
	if err = pipelineOne(result); err != nil {
		return err
	}
	return p.charge(ctx, tx, b, PipelineDemand{MetadataRows: 1}, -1)
}
