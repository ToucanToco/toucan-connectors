FROM python:3.10.6

RUN apt-get update -qq &&\
    apt-get install -y apt-transport-https curl libxmlsec1-openssl locales &&\
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - &&\
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list

# We install binaries needed by python libs
RUN apt-get update -qq &&\
    ACCEPT_EULA=Y apt-get install -fyq\
    libxmlsec1-dev freetds-dev \
    libxml2-dev libxslt-dev \
    libpq-dev libsasl2-dev \
    libsnappy-dev unixodbc-dev\
    msodbcsql17 libsnappy1v5 \
    libsasl2-modules libmagic1 \
    mime-support

# Generate locales:
RUN printf "en_US.UTF-8 UTF-8\nfr_FR.UTF-8 UTF-8\n" \
    > /etc/locale.gen && locale-gen

# langs configs
ENV LANG   en_US.UTF-8
ENV LC_ALL en_US.UTF-8
ENV TZ     UTC
ENV SLUGIFY_USES_TEXT_UNIDECODE="yes"

WORKDIR /src
COPY ./pyproject.toml ./poetry.lock ./

# pip and poetry setups
RUN pip install --upgrade pip poetry &&\
    poetry config virtualenvs.create false &&\
	poetry install -E all

# Add Tini
ADD ./.devcontainer/docker_configs/bashrc /root/.bashrc
ADD ./.devcontainer/docker_configs/ipython_startup.py /root/ipython_startup.py

ENTRYPOINT ["/bin/bash"]
