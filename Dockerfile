FROM python:3.9
WORKDIR /app
ENV TZ=Asia/Calcutta
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
ARG STATE_CODE=20
ARG LOG_FOLDER=logs
RUN mkdir $LOG_FOLDER
COPY log_saver.py bq-sql-cloudstorage.json ./
ENV LOG_FOLDER_ENV=${LOG_FOLDER}
ENV STATE_CODE_ENV=${STATE_CODE}
CMD ["python","-u","log_saver.py"]