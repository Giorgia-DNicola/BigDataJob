import time
import docker

def ensure_volume_exists(client, volume_name):
    """Create a volume if it doesn't exist already."""
    if not any(volume.name == volume_name for volume in client.volumes.list()):
        client.volumes.create(name=volume_name)

def build_image_if_not_exists(client, project_dir, dockerfile_name, tag_name):
    """Build a Docker image if it doesn't exist already."""
    image_exists = bool(client.images.list(name=tag_name))
    if not image_exists:
        client.images.build(path=project_dir, dockerfile=dockerfile_name, tag=tag_name)

def run_container_if_not_exists(client, image_name, container_name, links=None, volumes=None, detach=True):
    """Run a container if it doesn't exist already."""
    container_list = client.containers.list(all=True, filters={'name': container_name})
    if not container_list:
        container = client.containers.run(
            image_name,
            name=container_name,
            volumes=volumes,
            links=links,
            detach=detach
        )
        return container
    else:
        container = container_list[0]
        if container.status != 'running':
            container.start()
        return container

def wait_for_postgres(container, timeout=60):
    """Wait for PostgreSQL to be ready."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        if container.status != 'running':
            container.reload()
            time.sleep(2)
            continue
        exit_code, output = container.exec_run('pg_isready -U postgres')
        if exit_code == 0:
            return True
        time.sleep(2)
    raise Exception("PostgreSQL is not ready within the specified timeout.")
