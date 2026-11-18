package porcupine

import (
	"github.com/danielgtaylor/huma/v2"
	"regexp"
	"slices"
	"strings"

	"github.com/olup/porcupine/internal/apierr"
	"github.com/olup/porcupine/internal/auth"
)

func managementOperationAccess(op *huma.Operation) auth.OperationAccess {
	access, ok := op.Metadata["access"].(auth.OperationAccess)
	if !ok || op.OperationID == "" || !slices.Contains(auth.Permissions(), access.Permission) {
		panic("management operation requires an explicit valid access policy and operation ID")
	}
	for _, permission := range append(slices.Clone(access.AdditionalPermissions), access.InstancePermissions...) {
		if !slices.Contains(auth.Permissions(), permission) {
			panic("management operation uses an unknown permission")
		}
	}
	for _, parameter := range []string{access.ProjectParameter, access.TenantParameter} {
		if parameter != "" && !strings.Contains(op.Path, "{"+parameter+"}") {
			panic("management operation access policy references a missing path parameter")
		}
	}
	if access.TenantParameter != "" && !access.TenantAdmin && !access.TenantMember {
		panic("tenant path access requires an explicit tenant authorization rule")
	}
	return access
}

var sourceIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

func validSourceID(value string) bool { return value == "" || sourceIDPattern.MatchString(value) }

func validateCredentialSource(sourceID string, permissions, projects []string) error {
	if sourceID == "posthog" {
		return apierr.New(400, "source_id posthog is reserved for legacy ingestion")
	}
	if !validSourceID(sourceID) {
		return apierr.New(400, "invalid source_id")
	}
	if sourceID != "" && (len(projects) != 1 || projects[0] == "*" || !slices.Contains(permissions, "events:write")) {
		return apierr.New(400, "source_id requires events:write and exactly one project")
	}
	return nil
}

func delegatedSourceID(a auth.Principal, sourceID string, permissions, projects []string) (string, error) {
	if a.SourceID != "" {
		if sourceID != "" && sourceID != a.SourceID {
			return "", apierr.New(403, "cannot delegate a different source_id")
		}
		if slices.Contains(permissions, "events:write") {
			sourceID = a.SourceID
		}
	}
	if err := validateCredentialSource(sourceID, permissions, projects); err != nil {
		return "", err
	}
	return sourceID, nil
}
