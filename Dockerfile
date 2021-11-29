# Base image to use
FROM python:3.6 as build

# Install the C compiler tools
RUN apt-get update -y && \
  apt-get install build-essential -y && \
  pip install --upgrade pip

# Install libspatialindex
WORKDIR /tmp
RUN wget http://download.osgeo.org/libspatialindex/spatialindex-src-1.8.5.tar.gz && \
  tar -xvzf spatialindex-src-1.8.5.tar.gz && \
  cd spatialindex-src-1.8.5 && \
  ./configure && \
  make && \
  make install && \
  cd - && \
  rm -rf spatialindex-src-1.8.5* && \
  ldconfig
# upgrade setuptools_scm
RUN pip3 install --upgrade setuptools_scm wheel virtualenv


# setup workdir
WORKDIR /source_code
# copy from local disk to container - to path /source_code
COPY . .
RUN python -m venv venv
RUN . /source_code/venv/bin/activate
# env arguments for versioning
ARG VERSION=0.0.0
ENV VERSION=$VERSION
ENV SETUPTOOLS_SCM_PRETEND_VERSION=$VERSION
# install source code as local package
RUN  venv/bin/pip install --upgrade pip
#RUN venv/bin/pip install pyproj==1.9.6
RUN venv/bin/pip install --upgrade .
#RUN apk del .pynacl_deps build-base python3-dev libffi-dev cargo openssl-dev gcc musl-dev

# final app docker
#FROM python:3.6-alpine3.12
FROM python:3.6
# setup workdir
WORKDIR /source_code
COPY --from=build /source_code .
# add user: app and group app - application user
RUN chmod +x start.sh

RUN useradd -ms /bin/bash user && usermod -a -G root user
#RUN addgroup -S app && adduser -S app -G app
# create app directories
#RUN mkdir /opt/output && mkdir /opt/logs && mkdir /opt/jira
# app permissions
#RUN chmod +x start.sh && chown -R app:app /opt/output && chown -R app:app /opt/logs && chown -R app:app /opt/jira
# adding os (ping) functionality for operation system testing
# sets the user to run the application with: "app"
USER user
# cmd to run
CMD "/source_code/start.sh"

