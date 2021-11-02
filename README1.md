## compiel one module

remove example.ddr in the file MANIFEST.MF.
mvn -pl pljava-examples -am clean install

jar tf .\pljava-examples\target\pljava-examples-1.6-SNAPSHOT.jar

SELECT * FROM pg_language WHERE lanname LIKE 'java%';

pg_config --sysconfdir

docker cp ${Env:DOCKER_ID}:/opt/bitnami/postgresql/etc/pljava.policy ./

SET pljava.vmoptions TO '-Dorg.postgresql.pljava.policy.trial=file:/opt/bitnami/postgresql/etc/pljava.trail.policy'

## Copy to hostPath

minikube.exe cp .\pljava\pljava-examples\target\pljava-examples-1.6-SNAPSHOT.jar /mnt/data/pljava-examples-1.6-SNAPSHOT.jar

minikube.exe cp /opt/bitnami/postgresql/etc/pljava.policy

## in the hasura console.

SELECT sqlj.install_jar( 'file:///opt/jars/json-20210307.jar', 'json_20210307', true );

SELECT sqlj.install_jar( 'file:///opt/jars/pljava-examples-1.6-SNAPSHOT.jar', 'dcs_plugin', true );

SELECT sqlj.set_classpath('public', 'dcs_plugin:json_20210307');


SELECT sqlj.replace_jar( 'file:///opt/jars/pljava-examples-1.6-SNAPSHOT.jar', 'dcs_plugin', true );