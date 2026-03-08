import os
import time
from contextlib import contextmanager
from pathlib import Path

from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.waiting_utils import wait_for_logs
from testcontainers.localstack import LocalStackContainer

from aiotrino.constants import DEFAULT_PORT


MINIO_ROOT_USER = "minio-access-key"
MINIO_ROOT_PASSWORD = "minio-secret-key"

TRINO_DOCKER_REPOSITORY = os.environ.get("TRINO_DOCKER_REPOSITORY") or "trinodb/trino"
TRINO_VERSION = os.environ.get("TRINO_VERSION") or "latest"
TRINO_HOST = "localhost"

ARROW_SPOOLING_SUPPORTED = os.environ.get("TRINO_ARROW_SPOOLING_SUPPORTED", "true").lower() == "true"

def create_bucket(s3_client):
    bucket_name = "spooling"
    try:
        print("Checking for bucket existence...")
        response = s3_client.list_buckets()
        buckets = [bucket["Name"] for bucket in response["Buckets"]]
        if bucket_name in buckets:
            print("Bucket exists!")
            return
    except s3_client.exceptions.ClientError as e:
        if not e.response['Error']['Code'] == '404':
            print("An error occurred:", e)
            return

    try:
        print("Creating bucket...")
        s3_client.create_bucket(
            Bucket=bucket_name,
        )
        print("Bucket created!")
    except s3_client.exceptions.ClientError as e:
        print("An error occurred:", e)


@contextmanager
def start_development_server(port=None, trino_repository=TRINO_DOCKER_REPOSITORY, trino_version=TRINO_VERSION, support_arrow_spooling=ARROW_SPOOLING_SUPPORTED):
    """"
    The extra support_arrow_spooling parameter is necessary until an official Trino image is release with support for the new spooling parameters.
    """
    network = None
    localstack = None
    trino = None

    try:
        network = Network().create()
        supports_spooling_protocol = TRINO_VERSION == "latest" or int(TRINO_VERSION) >= 466
        if support_arrow_spooling:
            supports_spooling_protocol = True
        
        if supports_spooling_protocol:
            localstack = LocalStackContainer(image="localstack/localstack:latest", region_name="us-east-1") \
                .with_name("localstack") \
                .with_network(network) \
                .with_bind_ports(4566, 4566) \
                .with_bind_ports(4571, 4571) \
                .with_env("SERVICES", "s3")

            # Start the container
            print("Starting LocalStack container...")
            localstack.start()

            # Wait for logs indicating MinIO has started
            wait_for_logs(localstack, "Ready.", timeout=30)

            # create spooling bucket
            create_bucket(localstack.get_client("s3"))

        trino = DockerContainer(f"{trino_repository}:{trino_version}") \
            .with_name("trino") \
            .with_network(network) \
            .with_env("TRINO_CONFIG_DIR", "/etc/trino") \
            .with_bind_ports(8080, DEFAULT_PORT)

        root = Path(__file__).parent.parent

        trino = trino \
            .with_volume_mapping(str(root / "etc/catalog"), "/etc/trino/catalog")

        # Enable spooling config
        if supports_spooling_protocol:
            trino \
                .with_volume_mapping(
                    str(root / "etc/spooling-manager.properties"),
                    "/etc/trino/spooling-manager.properties", "rw") \
                .with_volume_mapping(str(root / "etc/jvm.config"), "/etc/trino/jvm.config") \
                .with_volume_mapping(str(root / f"etc/config{'-arrow' if support_arrow_spooling else ''}.properties"), "/etc/trino/config.properties")
        else:
            trino \
                .with_volume_mapping(str(root / "etc/jvm-pre-466.config"), "/etc/trino/jvm.config") \
                .with_volume_mapping(str(root / "etc/config-pre-466.properties"), "/etc/trino/config.properties")

        print(f"Starting Trino container using image {trino_repository}:{trino_version}...")
        trino.start()

        # Wait for logs indicating the service has started
        
        wait_for_logs(trino, "SERVER STARTED", timeout=60)

        # Otherwise some tests fail with No nodes available
        time.sleep(2)

        yield localstack, trino, network
    finally:
        # Stop containers when exiting the context
        if trino:
            try:
                print("Stopping Trino container...")
                trino.stop()
            except Exception as e:
                print(f"Error stopping Trino container: {e}")

        if localstack:
            try:
                print("Stopping LocalStack container...")
                localstack.stop()
            except Exception as e:
                print(f"Error stopping LocalStack container: {e}")

        if network:
            try:
                print("Removing network...")
                network.remove()
            except Exception as e:
                print(f"Error removing network: {e}")


def main():
    """Run Trino setup independently from pytest."""
    with start_development_server(port=DEFAULT_PORT):
        print(f"Trino started at {TRINO_HOST}:{DEFAULT_PORT}")

        # Keep the process running so that the containers stay up
        input("Press Enter to stop containers...")


if __name__ == "__main__":
    main()
