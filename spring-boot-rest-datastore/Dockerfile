FROM java:8
ENV BASHRC_GOOGLE_PATH=/google/devshell/bashrc.google \
USER=sandeep_pratap \
BROWSER=echo \
DEVSHELL_GCLOUD_CONFIG=cloudshell-25406 \
CUSTOM_ENVIRONMENT=false \
DEVSHELL_CLIENT_PORT=8998 \
GCE_METADATA_HOST=169.254.169.254 \
TMUX_PANE=%0 \
DEVSHELL_CLIENTS_DIR=/var/run/google/devshell \
SHLVL=3 \
LOGNAME=sandeep_pratap \
GOOGLE_CLOUD_PROJECT=my-project-b8849 \
HISTSIZE=1000 \
HISTFILESIZE=1000 \
DEVSHELL_IP_ADDRESS=35.240.223.36 \
DEVSHELL_SERVER_URL=https://ssh.cloud.google.com
VOLUME /opt
WORKDIR "/opt"
ADD target/spring-boot-rest-datastore-0.0.1-SNAPSHOT.jar .
ENTRYPOINT ["java","-jar","spring-boot-rest-datastore-0.0.1-SNAPSHOT.jar"]
