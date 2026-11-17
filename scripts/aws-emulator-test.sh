#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
pattern=${1:-'^TestDuckLakeS3(SharedPostgresNativeImportPublicationVersioned|HTTPJourneyRestartAndProjectDeletion)$'}
port=${PORCUPINE_AWS_EMULATOR_PORT:-19297}
image=porcupine-aws-emulator:local
bucket=porcupine-ducklake-test-emulator
endpoint="https://127.0.0.1:$port"

if ! docker image inspect "$image" >/dev/null 2>&1; then
  case "$(docker info --format '{{.Architecture}}')" in
    aarch64|arm64) package_arch=aarch64 ;;
    x86_64|amd64) package_arch=x86_64 ;;
    *) echo "AWS Emulator needs an Arm64 or x86-64 Docker host." >&2; exit 1 ;;
  esac

  # AWS publishes native packages rather than a prebuilt Docker image.
  docker build -t "$image" - <<DOCKERFILE
FROM public.ecr.aws/amazonlinux/amazonlinux:2023-minimal
ADD https://downloads.aws.com/aws-emulator/packages/aws-emulator/LTS/latest/aws-emulator-$package_arch.rpm /tmp/aws-emulator.rpm
RUN rpm -i /tmp/aws-emulator.rpm && rm /tmp/aws-emulator.rpm && mkdir -p /home/emulator && chown 1000 /home/emulator
USER 1000
ENV HOME=/home/emulator
EXPOSE 9297
ENTRYPOINT ["aws-emulator", "-b", "0.0.0.0"]
DOCKERFILE
fi

fixture_home=$(mktemp -d)
container=
cleanup() {
  if [[ -n $container ]]; then docker stop "$container" >/dev/null || true; fi
  rm -rf "$fixture_home"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
mkdir -p "$fixture_home/.aws/emulator"
container=$(docker run --pull=never --detach --rm --memory=2g --memory-swap=2g \
  --publish "127.0.0.1:$port:$port" --user "$(id -u):$(id -g)" \
  --env "HOME=$fixture_home" \
  --volume "$fixture_home/.aws/emulator:$fixture_home/.aws/emulator" \
  "$image" --port "$port" --telemetry off)

fixture="$fixture_home/.aws/emulator/$port"
ca="$fixture/ca.pem"
credentials="$fixture/emulator_credentials"
health() {
  curl --noproxy '*' --cacert "$ca" --connect-timeout 1 --max-time 2 -fsS "$endpoint/__admin/health"
}
for ((attempt=0; attempt<30; attempt++)); do
  if health >/dev/null 2>&1; then break; fi
  sleep 1
done
health >/dev/null
docker exec "$container" aws-emulator --version

isolated=(
  env -i "PATH=$PATH" "HOME=$HOME" "TMPDIR=${TMPDIR:-/tmp}"
  AWS_CONFIG_FILE=/dev/null "AWS_SHARED_CREDENTIALS_FILE=$credentials"
  AWS_EC2_METADATA_DISABLED=true AWS_DISABLE_HOST_PREFIX_INJECTION=true
  "AWS_ENDPOINT_URL=$endpoint"
)
"${isolated[@]}" aws --endpoint-url "$endpoint" --ca-bundle "$ca" --region us-east-1 --no-cli-pager \
  s3api create-bucket --bucket "$bucket" >/dev/null

access_key=$(awk -F'= *' '/^aws_access_key_id/ {print $2}' "$credentials")
secret_key=$(awk -F'= *' '/^aws_secret_access_key/ {print $2}' "$credentials")
test -n "$access_key"
test -n "$secret_key"

# The Go client installs its own TLS transport. Pass the CA through the fixture.
"${isolated[@]}" \
  "PORCUPINE_TEST_DUCKLAKE_S3_ENDPOINT=$endpoint" \
  "PORCUPINE_TEST_DUCKLAKE_S3_BUCKET=$bucket" \
  "PORCUPINE_TEST_DUCKLAKE_S3_CA_CERT_FILE=$ca" \
  PORCUPINE_TEST_DUCKLAKE_S3_REGION=us-east-1 \
  "PORCUPINE_TEST_DUCKLAKE_S3_ACCESS_KEY_ID=$access_key" \
  "PORCUPINE_TEST_DUCKLAKE_S3_SECRET_ACCESS_KEY=$secret_key" \
  CGO_ENABLED=0 go run ./internal/cmd/shared-postgres-integration -- \
    go test ./internal/porcupine -count=1 -timeout=180s -v \
    -run "$pattern"
