# Getting Started

### OKTA 

1. Sign in to your Okta organization with your administrator account.
2. Go to Applications > Applications.
3. Click Create App Integration.
4. Select OIDC - OpenID Connect as the Sign-in method.
5. Select Web Application as the Application type and click Next.
6. Enter a name for your app integration (or leave the default value).
7. Enter values for the Sign-in redirect URI: 
http://localhost:8080/login/oauth2/code/okta
8. Add the Base URI of your application during local development
http://localhost:8080
9. Assign the group that you want: Everyone default.

Run kafka manually:
```sh
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic myTopic --partitions 1 --replication-factor 1
```
Or run container:
```sh
docker-compose -f ./docker-compose.yml up -d
```

Run Spring:
./mvnw spring-boot:run

Send message:
```sh
http://localhost:8080/kafka/produce?msg=This is my message again
http://localhost:8080/kafka/messages to read messages.
```
Or curl:
```sh
curl -v "http://localhost:9000/kafka/produce?msg=test"
```