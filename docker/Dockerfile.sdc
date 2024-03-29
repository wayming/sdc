# For more information, please refer to https://aka.ms/vscode-docker-python
FROM ubuntu:latest

RUN apt update

RUN apt install git unzip wget -y
RUN apt install curl iproute2 vim -y
RUN apt install postgresql-client -y
RUN rm -rf /var/lib/apt/lists/*

# Install Golang
ENV GOLANG_VERSION 1.22.1
RUN wget https://golang.org/dl/go$GOLANG_VERSION.linux-amd64.tar.gz && \
    tar -C /usr/local -xzf go$GOLANG_VERSION.linux-amd64.tar.gz && \
    rm go$GOLANG_VERSION.linux-amd64.tar.gz


# # Creates a non-root user with an explicit UID and adds permission to access the /app folder
# # For more info, please refer to https://aka.ms/vscode-docker-python-configure-containers
ARG USERNAME=appuser
ARG USER_UID=1000
ARG USER_GID=$USER_UID

RUN groupadd --gid $USER_GID $USERNAME \
    && useradd --uid $USER_UID --gid $USER_GID -m $USERNAME \
    && usermod -aG video $USERNAME \
    && apt-get update \
    && apt-get install -y sudo \
    && echo "$USERNAME ALL=(ALL:ALL) NOPASSWD:ALL" > /etc/sudoers.d/$USERNAME \
    && chmod 0440 /etc/sudoers.d/$USERNAME

USER $USERNAME
WORKDIR /home/$USERNAME

ENV PATH=$PATH:/usr/local/go/bin

CMD sleep infinitly