docker run \
    -it \
    --rm \
    -v /etc/passwd:/etc/passwd \
    -v /home/$USER:/home/$USER \
    -u $(id -u ${USER}) \
    --name=prestissimo-environment \
    --mount type=bind,source=${PWD}/..,target=/src \
    --workdir=/src \
    prestissimo/environment:latest bash