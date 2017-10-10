FROM node:8.6-slim

RUN mkdir /src

WORKDIR /src

# make sure local bucket and user.json file are already created.
COPY . /src


RUN cd /src
RUN rm -rf node_modules/
RUN npm install

EXPOSE 7000

CMD ["node", "main.js"]