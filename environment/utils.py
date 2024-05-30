import docker

def ensure_volume_exists(client, volume_name):
    """Crea un volume se non esiste gia'."""
    if not any(volume.name == volume_name for volume in client.volumes.list()):
        client.volumes.create(name=volume_name)

def build_image_if_not_exists(client, project_dir, dockerfile_name, tag_name):
    """Costruisce un'immagine Docker se non esiste gia'."""
    image_exists = bool(client.images.list(name=tag_name))
    if not image_exists:
        client.images.build(path=project_dir, dockerfile=dockerfile_name, tag=tag_name)

def run_container_if_not_exists(client, image_name, container_name, links=None, volumes=None):
    """Esegue un container se non esiste gia'."""
    container_exists = bool(client.containers.list(filters={'name': container_name}))
    if not container_exists:
        client.containers.run(
            image_name,
            name=container_name,
            volumes=volumes,
            links=links,
            detach=True
        )
