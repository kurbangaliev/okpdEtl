FROM apache/airflow
#ADD google-chrome-stable_current_amd64.deb .
#ADD chromedriver .
USER root
RUN usermod -aG sudo airflow
RUN echo "airflow:123123" | chpasswd
RUN echo 'airflow ALL=(ALL:ALL) ALL' >> /etc/sudoers
RUN  apt-get update \
  && apt-get install -y wget \
  && rm -rf /var/lib/apt/lists/* \
#RUN chmod +x ./chromedriver
#RUN mv -f ./chromedriver /usr/local/share/chromedriver
#RUN ln -s /usr/local/share/chromedriver /usr/local/bin/chromedriver
#RUN ln -s /usr/local/share/chromedriver /usr/bin/chromedriver
#RUN dpkg --force-all --force-depends -i google-chrome-stable_current_amd64.deb
#RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | sudo apt-key add -
#RUN sh -c 'echo "deb https://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list'
#RUN apt -y update
#RUN apt -y install google-chrome-stable

#RUN apt-get -y install chromium-chromedriver

USER airflow
ADD requirements.txt .
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt

