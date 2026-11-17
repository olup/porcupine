package porcupine

import (
	"slices"
	"testing"

	lakeapi "github.com/olup/porcupine/internal/lake"
)

func TestDuckLakeS3TenantDeletionManagedObjectsPreserveB(t *testing.T) {
	t.Parallel()
	fixture := nativeS3Fixture(t)
	s, _ := nativeS3Service(t, fixture)
	defer s.Close()
	a, b := tenantTestCreate(t, s, "A"), tenantTestCreate(t, s, "B")
	pa, ka := tenantJourneyProject(t, s, a)
	pb, kb := tenantJourneyProject(t, s, b)
	request(t, s, "POST", "/capture/", "", captureBody(ka, "a-s3"), 200)
	request(t, s, "POST", "/capture/", "", captureBody(kb, "b-s3"), 200)
	duckLakeJourneyDrain(t, s)
	store := s.lake.(*lakeapi.DuckLakeStore)
	fixture.flush(t, store, pa)
	fixture.flush(t, store, pb)
	prefixA, prefixB := fixture.projectPrefix(t, store, pa), fixture.projectPrefix(t, store, pb)
	beforeA, beforeB := fixture.keys(t, prefixA), fixture.keys(t, prefixB)
	if len(beforeA) == 0 || len(beforeB) == 0 {
		t.Fatal("fixture did not create managed Parquet for both tenants")
	}
	request(t, s, "DELETE", "/api/v1/tenants/"+a, adminToken, "", 202)
	status := waitTenantDeletion(t, s, a, "logically_deleted")
	if status.PhysicalPurge != "pending_verification" {
		t.Fatal("current-object absence claimed version purge", status)
	}
	if after := fixture.keys(t, prefixA); len(after) != 0 {
		t.Fatal("A current managed S3 objects survived", after)
	}
	if after := fixture.keys(t, prefixB); !slices.Equal(after, beforeB) {
		t.Fatal("A deletion changed B S3 inventory", after, beforeB)
	}
	if kept, err := store.Load(t.Context(), pb, "b-s3"); err != nil || kept.ProjectID != pb {
		t.Fatal("B became unreadable", kept, err)
	}
}
