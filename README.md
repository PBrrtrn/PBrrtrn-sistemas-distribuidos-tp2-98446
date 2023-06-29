# Bike Rides Analyzer
## TP2 de Sistemas Distribuidos (75.74) - FIUBA
### Pablo Berrotarán (98446) y Andrés Zambrano

**Bike Rides Analyzer** es un sistema destinado a la recolección y análisis de grandes cantidades de datos sobre el uso de estaciones de bicicleta en las ciudades de Montreal, Toronto y Washington. **Bike Rides Analyzer** cumple con necesidades de escalabilidad implementando un sistema de calculo distribuido construido sobre RabbitMQ que distribuye y paraleliza la carga de cómputo a lo largo de varias unidades de procesamiento.

Además, el sistema muestra alta disponibilidad al cliente, siendo este capaz de recuperarse de fallas y continuar ejecutando y respondiendo pedidos de clientes incluso cuando los nodos que conforman su red de procesamiento encuentran irregularidades en su comportamiento.

## Alcance
El sistema propuesto apunta a resolver de forma asincrónica consultas provenientes de los distintos clientes que se conectan al mismo. Aquellos clientes que tengan intenciones de consumir el sistema deberían conectarse al sistema usando un programa cliente y enviar sus datos, los cuales serán procesados y agregados para eventualmente dar una respuesta al cliente en cuestión. Las consultas que el sistema debe poder responder son:
- La duración promedio de viajes que iniciaron en días con >30mm de precipitaciones
- Los nombres de estaciones que al menos duplicaron la cantidad de viajes iniciados en ellas entre 2016 y 2017
- Los nombres de estaciones de Montreal para las cuales el promedio de ciclistas recorren más de 6km en llegar a ellas

Para responder a estas consultas, el sistema requiere que los clientes realicen la entrada de datos correspondientes a clima, seguido de los datos de estaciones, y finalmente los datos de los viajes. Para obtener la data del cliente, se utilizó el dataset disponible en Kaggle  [Public bike sharing in North America](https://kaggle.com/datasets/jeanmidev/public-bike-sharing-in-north-america).

El formato de las líneas a ingresar al cliente para su posterior análisis debe ser el siguiente:
- Para las estaciones: `code, name, latitude, longitude, yearid`
- Para el registro climático: `date, prectot, qv2m, rh2m, ps, t2m_range, ts, t2mdew, t2mwet, t2m_max, t2m_min, t2m, ws50m_range, ws10m_range, ws50m_min, ws10m_min, ws50m_max, ws10m_max, ws50m, ws10m, yearid`
- Para los viajes: `start_date, start_station_code, end_date, end_station_code, duration_sec, is_member, yearid`

Para facilitar el escalamiento horizontal del sistema, se busca que el agregar nodos adicionales para procesamiento de consultas sea lo menos doloroso y lo más accesible posible. Estos nodos deben poder caerse del sistema sin que el mismo deje de aparentar alta disponibilidad, de modo tal que si uno cae el resto pueda seguir operando normalmente con capacidades disminuidas hasta que el nodo caído se reincorpore a la red. La eventual aparición de fallas en el sistema debería manifestarse como un aumento en el tiempo de ejecución de las consultas, y no como un evento irrecuperable que cause que se tenga que re-ejecutar una consulta.

## Arquitectura
El front-end del sistema se compone de un programa cliente que sirve de punto de entrada para el usuario tanto para nutrir el sistema de datos como para hacer el pedido de las consultas. El programa cliente se encarga de leer los datos de entrada y enviarlos mediante un socket standard TCP a un programa que hace las de servidor de ingesta de datos. El servidor de ingesta de datos luego se ocupa de distribuir los datos recibidos del cliente al resto del sistema, conectándose a un broker de RabbitMQ y usando las colas del mismo para garantizar la entrega de los datos. Por otro lado, el back-end comprende varios tipos distintos de procesos que se nutren e informan resultados entre ellos mediante colas de RabbitMQ. El cliente puede ser configurado para enviar los datos en batches de distinto tamaño, a modo de aminorar la cantidad de operaciones de send y receive realizadas.

### Red de procesamiento
El backend tolera que en el despliegue se declaren nodos adicionales a modo de paralelizar el trabajo y escalar acorde al volumen de datos recibidos. 

### Red de supervisores
Los nodos que integran la red de procesamiento del backend pueden salir y volver a entrar al sistema en la medida que sucedan fallas; este comportamiento es logrado haciendo que todos los nodos formen, en simultáneo, una red de supervisores. Dicha red de supervisores utiliza el algoritmo _Bully_ de elección de líder para elegir un supervisor al cual los seguidores deberían reportar que siguen vivos mediante una señal de _HEARTBEAT_. Si en algún momento el líder detecta que un nodo no está respondiendo, se utiliza la API de cliente de Docker para reiniciar su container.

Si en algún momento el nodo que deja de responder es el líder, los seguidores deben elegir un nuevo líder entre ellos hasta que el líder original pueda volver a supervisar la red. Para esto, los seguidores esperan también recibir una respuesta _HEARTBEAK_ACK_, asumiendo que si no se recibe después de una cierta cantidad de tiempo, el líder ha muerto. En un caso ideal, luego de notar que el líder está inactivo, el próximo líder de inmediato lo reiniciaría y cedería el control, pero en caso de que por algún motivo no fuera posible revivir el antiguo líder, la red de supervisores no podría seguir funcionando hasta que el líder original pueda ser reiniciado, lo cual, de seguir cayendo nodos, haría que el sistema se frene.

Teniendo en cuenta que una configuración con "poco escalamiento" (paralelismo de no más de dos nodos por tipo) tendrá 16 nodos en la red de supervisión, se entiende que la probabilidad de que todos los nodos fallen en simultáneo, no permitiendo que haya al menos un supervisor para levantar a sus compañeros, es extremadamente baja.

### Comunicación entre procesos
La comunicación entre el cliente y el backend se lleva a cabo mediante una conexión punto a punto con TCP. Mientras tanto, los procesos que componen el backend usan el middleware RabbitMQ para comunicarse entre ellos con colas de mensajes, permitiendo así un mejor asincronismo en la distribución de datos y cálculos realizados.

A fin de permitir atender múltiples clientes, los nodos de procesamiento hacen una división lógica entre nodos Stateful y nodos Stateless. Los nodos Stateless son aquellos que no necesitan guardar data de clientes, si no que simplemente reciben datos, los procesan y los forwardean a sus peers. En cambio, los nodos Stateful deben poder ingerir datos de algunos clientes al mismo tiempo que responden a los pedidos de otros cuya ingesta ya haya terminado. Para contemplar este caso, el servidor que se comunica con el cliente anuncia clientes nuevos mediante una cola de fanout para que los nodos de tipo Stateful inicien un proceso ocupado de atender al nodo. Ese proceso utilizará un esquema publisher-subscriber, consumiendo únicamente datos y pedidos del nodo correspondiente. Este esquema requiere colaboración por parte de los nodos Stateless -- Cuando un nodo Stateless forwardea un dato procesado para que sea almacenado, lo encola con la clave de enrutamiento necesaria para que el dato sea almacenado junto al resto de los datos provenientes de ese cliente.

![Comunicación entre nodos de procesamiento y de almacenamiento](https://github.com/PBrrtrn/PBrrtrn-sistemas-distribuidos-tp2-98446/blob/master/.img/stateless_and_stateful_pubsub.png)

## Casos de uso
El cliente interactua con el sistema mediante seis casos de uso posibles. Como puede verse en el siguiente diagrama, los casos de uso se dividen en tres que corresponden a la entrada de datos al sistema, y tres casos que ejecutan las consultas al sistema:

![Diagrama de casos de uso](https://github.com/PBrrtrn/PBrrtrn-sistemas-distribuidos-tp2-98446/blob/master/.img/use_case_diagram.png)

Es importante notar que para ejecutar las queries, deben primero haberse ingresado los datos correspondientes a estaciones, clima y viajes, de modo que las queries se ejecuten una vez que todos estos elementos hayan sido procesados. Esta secuencia de entrada de datos puede apreciarse en la forma de un **grafo dirigido acíclico (DAG)** que muestra las dependencias de tareas previas para calcular cada query.

![Grafo dirigido acíclico](https://github.com/PBrrtrn/PBrrtrn-sistemas-distribuidos-tp2-98446/blob/master/.img/dag.png)

## Vista física
El flujo de datos en el sistema es manejado por nodos que se comunican mediante middlewares de colas distribuidas. Para que el sistema sea funcional, se necesita dar de alta al menos un nodo de cada tipo, aunque es posible dar de alta más de un nodo de un tipo a fin de escalar junto a la demanda. Cada uno de estos nodos se encuentran conectados al broker de RabbitMQ, de modo que puedan consumir mensajes de otros nodos que conozcan las colas de las cuales toman su entrada de datos:

![Diagrama de despliegue](https://github.com/PBrrtrn/PBrrtrn-sistemas-distribuidos-tp2-98446/blob/master/.img/deployment_diagram.png)

Nótese que en el diagrama de despliegue no se incluyen el módulo common, ya que sus contenidos son comunes a todos los nodos, pero todos los nodos deben ser desplegados con el módulo Supervisor para formar su red de supervisores.

Para la entrada de datos, el nodo de ingesta de datos recibe los registros correpondientes a estaciones, clima y viajes y los distribuye a lo largo del sistema para que sean analizados y finalmente almacenados para poder construir la respuesta a las consultas.

Las estaciones ingresan al sistema y son encoladas. De a una, un nodo StationsManager desencola y construye un índice con las estaciones por código para almacenarlas. En la misma cola de la que se reciben los registros de estaciones, se recibe también una señal de EOF para indicar que el nodo StationsManager ha recibido todas las estaciones y puede empezar a escuchar pedidos de información sobre las mismas.

Luego ingresan los registros climaticos, que van a parar a una cola de la cual pueden desencolar N nodos WeatherFilter. Al desencolar un registro de clima, el WeatherFilter filtra aquellos que tienen menos de 30mm de precipitaciones, y encola los demás para que sean desencolados por un nodo WeatherManager que los indexa por fecha.

Los registros de viajes son depositados en un exchange de tipo Fanout para que sean procesados por nodos de distintos tipos.

Para la query de estaciones de montreal con distancia de viaje promedio mayor a 6km, el primer nodo que interviene se ocupa de filtrar los viajes por ciudad, descartando aquellos que no corresponden a Montreal. Luego se hace una operación de join de los viajes pasando por el StationManager para obtener las estaciones de cada viaje, y otro a un nodo de tipo DistanceCalculator que se dedica a calcular distancias. Finalmente, los registros de viajes con las distancias recorridas se pasan a un nodo DistanceRunningAvg que mantiene las distancias promedio recorridas para llegar a cada estación de Montreal. Cuando llega el EOF de viajes, el nodo DistanceRunningAvg empieza a escuchar llamados por RPC para devolver aquellas estaciones que hayan promediado más de 6km.

Para la query que busca estaciones que hayan duplicado sus viajes de 2016 a 2017, los viajes van a parar a una cola de la cual desencola un nodo que filtra los viajes que no hayan sido en esos años, y los pasa con una cola directa a un nodo que lleva la cuenta de cuantos viajes en cada año recibió cada estación. Al recibir la señal de EOF de viajes, el nodo que lleva la cuenta empieza a escuchar para recibir un pedido por RPC y, al recibirlo, selecciona de su almacenamiento interno aquellas estaciones que hayan duplicado la cantidad de viajes, hace un request al StationsManager por sus nombres, y los devuelve.

Finalmente, para responder la query de duración de viajes con precipitaciones, los viajes son desencolados por un nodo que llama al WeatherManager para que este filtre aquellos que no hayan tenido >30mm de precipitaciones. Acto seguido, los viajes pasan a un nodo DurationRunningAvg que mantiene el promedio de duración total para todos esos viajes con precipitaciones. Al recibir una señal de EOF de los viajes, el nodo DurationRunningAvg escucha por un llamado de RPC para devolver el promedio final.

La distribución de los viajes a modo de nutrir a los nodos StationDistanceRunningAvg, DurationRunningAvg y ByYearAndStationTripsCount puede apreciarse en el siguiente diagrama de robustez:


![Diagrama de robustez](https://github.com/PBrrtrn/PBrrtrn-sistemas-distribuidos-tp2-98446/blob/master/.img/robustness_diagram.png)

Al ejecutar las consultas, se envía mensajes a los tres nodos que almacenan las agregaciones de datos. StationDistanceRunningAvg y DurationRunningAvg calculan sus resultados inmediatamente y los devuelven, mientras que ByYearAndStationTripsCount debe primer hacer un llamado a StationsManager para buscar los nombres de las estaciones que apliquen.

## Vista estática

En el núcleo de la estructura de objetos del programa se encuentra la clase `ProcessingNode`, que representa un nodo de la red en ejecución. Al instanciar un ProcessingNode, este debe componer una clase QueueConsumer, y un SupervisorProcess, a modo de encapsular dentro de estos colaboradores la acción de ingesta, procesamiento y salida de datos, y la acción de supervisión de nodos en la red, respectivamente.

Todo `QueueConsumer` se compone de una cola de la cual se leen los mensajes a procesar, una callback de procesamiento con la cual procesar los mensajes, y un OutputProcessor que maneja la salida.

La jerarquía de los `OutputProcessor` se divide en tres clases:
- `ForwardingOutputProcessor` es usado exclusivamente por los nodos de tipo stateless, solamente encolando (guardando en disco el estado a modo de poder recuperarlo) el resultado de la callback de procesamiento de input.
- `StorageOutputProcessor` guarda el resultado del procesamiento del input en un estado persistente y recuperable a fallas, componiendo el mismo a partir de los datos que llegan provenientes de la ingesta. Cuando la ingesta de datos termina, evento que se señaliza con la llegada de N señales de EOF, el StorageOutputProcessor comienza a ejecutar su propio QueueConsumer a modo de leer una cola de pedidos RPC leyendo del estado final de su storage.

## Vista de desarrollo
La completitud del código, tanto el cliente como los componentes del sistema de cálculo distribuido y los scripts de infraestructura de colas, se encuentra en un único repositorio de Git.

Dentro del repositorio, el código se encuentra dividido en cuatro directorios: frontend, backend, common y test.

![Diagrama de paquetes del repositorio](https://github.com/PBrrtrn/PBrrtrn-sistemas-distribuidos-tp2-98446/blob/master/.img/fd_overview.png)

Por su parte el directorio **frontend** cuenta con un main.py, un objeto cliente, y un objeto batch_reader para facilitar la lectura de archivos. Dentro del directorio se encuentran también los archivos dockerfile y de configuración necesarios para lanzar un proceso cliente. Frontend importa utilidades de parseo que, por conveniencia, se guardan en el módulo Common.

El directorio **backend** se divide en trece subdirectorios con archivos de configuración e inicialización para poner a ejecutar un nodo de procesamiento configurado para cumplir con la funcionalidad del nodo en cuestión, así como los Dockerfile necesarios para hacer su despliegue. En cada `main` se inicializa un StatefulNode o un StatelessNode con sus correspondientes componentes y se lo pone a correr, inyectándole las capas de modelo y comunicación.

**common** contiene utilidades varias para dar contexto al ambiente de los procesos en el archivo `env_utils.py`, utilidades varias usadas por diversas clases del sistema, y módulos con los distintos componentes que deben inicializarse en un nodo para configurar su comportamiento. `model` contiene objetos de la lógica del dominio propuesto, y `network` aloja utilidades de la capa de comunicación, como ser constantes y funciones de serialización. El módulo `supervisor` es donde están las implementaciones de los algoritmos de elección de líder y supervisor, utilidades de comunicación específicas de los supervisores, y objetos que manejan el cliente de Docker responsable de reiniciar containers.

El módulo más extenso dentro de **common** es `processing_node`. En este nodo se encuentran las abstracciones que permiten construir un nodo de procesamiento: StatefulNode, StatelessNode, QueueConsumer, los distintos OutputProcessor y funciones universales de ProcessInput, además de los StorageHandler, abstracciones usadas para persistir el estado de un nodo al recuperarse de una falla.

## Vista de procesos/dinámica

### Cliente

El proceso cliente comienza a ejecutar inicializando para cada archivo de cada ciudad una instancia del objeto BatchReader, el cual permite leer batches de lineas del archivo mediante sucesivos llamados a `read` y convertirlas en objetos del modelo pasando una callback de deserialización. El cliente inicializa la conexión con el cliente y envía al servidor la data de cada uno de los archivos por socket.

Para enviar la data de cada ciudad:
- Primero se envía un símbolo START definido en `common.network.constants` seguido del largo del nombre de la ciudad, seguido de la ciudad a la cual corresponde la data enviada
- Luego se envía, tantas veces como sea necesario, un símbolo BATCH seguido de la data serializada del batch
- Al terminar de enviar la data correspondiente a una ciudad, se envía un símbolo END seguido del largo del nombre de la ciudad, seguido del nombre de la ciudad
- Al terminar de enviar la data de todas las ciudades, se envía un símbolo END_ALL

La clase WrappedSocket provee envío seguro evitando short reads y short sends en la comunicación entre el cliente y el backend

Al terminar de enviar la data, el cliente envía un mensaje EXECUTE_QUERIES y queda esperando recibir tres mensajes provenientes del servidor con los resultados de cada consulta prefijados por un símbolo `MONTREAL_STATIONS_OVER_6KM_AVG_TRIP_DISTANCE_RESULT`, `WITH_PRECIPITATIONS_AVG_TRIP_DURATION_RESULT` y `WITH_PRECIPITATIONS_AVG_TRIP_DURATION_RESULT`

Finalmente el cliente imprime los resultados, cierra el socket y termina de ejecutar.

![Diagrama de secuencia general del protocolo](https://github.com/PBrrtrn/PBrrtrn-sistemas-distribuidos-tp2-98446/blob/master/.img/protocol_sequence_diagram.png)

Al ingresar los viajes, estos son enviados a un nodo _MontrealStationsOver6kmAvgTripDistanceIngestor_, el cual filtra los viajes quedandose solo con los batches de Montreal. Acto seguido, el nodo hace un llamado al StationManager para obtener las estaciones de llegada y de partida para ese viaje. Al recibir un viaje, el StationNode lo matchea con su índice de estaciones en un Left Join y lo devuelve con las estaciones. Con ese resultado, el nodo lo envía a un nodo DistanceCalculator para obtener la distancia del viaje, y finalmente todo llega al nodo DistanceRunningAvg. Al terminar de recibir viajes, el nodo de ingesta de datos avisa al DistanceNode y al StationsManager que llegó un EOF para que estos pueden procesarlo.

El DistanceNode terminará al recibir la señal de EOF, pero el StationsManager solo saldrá cuando haya recibido señales de EOF por parte del nodo de ingesta actual, así como el de estaciones que hayan duplicado sus viajes.

![Diagrama de secuencia de los viajes para calcular la distancia promedio de viaje de las estaciones de Montreal](https://github.com/PBrrtrn/PBrrtrn-sistemas-distribuidos-tp2-98446/blob/master/.img/montreal_stations_over_6km_average_sequence.png)

Para ejecutar la Query 3, el cliente envía la consulta al ByStationRunningAvgNode, que calcula el promedio de todas las estaciones de Montreal que guardó, y devuelve las que hayan registrado un viaje promedio mayor a 6km.

![Diagrama de actividad de los viajes para la Query 3](https://github.com/PBrrtrn/PBrrtrn-sistemas-distribuidos-tp2-98446/blob/master/.img/avg_montreal_trip_distance_per_station_activity.png)

Los viajes también son forwardeados a los ByYearFilterNode, donde son filtrados para quedarse sólo con aquellos de los años 2016 y 2017. Acto seguido, los viajes de 2016 y 2017 pasan a un ByStationYearlyTripsNode, que lleva la cuenta de los viajes en cada estación por año, indexandolas primero por ciudad y luego por código de estación.

![Diagrama de secuencia de los viajes para la Query 2](https://github.com/PBrrtrn/PBrrtrn-sistemas-distribuidos-tp2-98446/blob/master/.img/stations_doubled_yearly_trips_sequence.png)

Al ejecutar la Query de estaciones que hayan duplicado el número de viajes anuales entre 2016 y 2017, se envía el pedido al ByStationYearlyTripsNode. En respuesta, el nodo recurre su índice de viajes por estación y se queda sólo con los códigos de aquellas que hayan duplicado sus viajes. Luego, pide a los StationsNode hacer el join de los códigos con los nombres, y devuelve la lista de nombres al cliente que realizó la consulta.

![Diagrama de secuencia para la ejecución de la consulta](https://github.com/PBrrtrn/PBrrtrn-sistemas-distribuidos-tp2-98446/blob/master/.img/stations_doubled_yearly_trips_query.png)

Los registros de clima se propagan desde el servidor de ingesta al sistema pasando por filtros de weather que solo deja pasar aquellos registros con >30mm de precipitaciones, y de ahí pasan al WeatherManager. Al llegar la señal de EOF, los filtros la forwardean al manager y cierran su ejecución. Al recibir una señal de EOF por cada filtro, el WeatherManager empieza a atender pedidos por RPC.

![Diagrama de secuencia para la propagacion de registros climaticos](https://github.com/PBrrtrn/PBrrtrn-sistemas-distribuidos-tp2-98446/blob/master/.img/weather_ingestion_sequence.png)

Al entrar viajes, pasan a un nodo que hace pedidos por RPC a WeatherManager. WeatherManager responde haciendo un Inner Join y devolviendo los viajes que hayan sucedido en una fecha con >30mm de precipitaciones. Al recibir los viajes, el nodo de ingesta los pasa a un nodo que lleva el running average para todos los viajes recibidos. Al recibir una señal de EOF, el nodo de ingesta la pasa al running average para que empiece a recibir pedidos por RPC.

![Diagrama de secuencia para la propagacion de viajes para la consulta de duración de viajes con >30mm de precipitaciones](https://github.com/PBrrtrn/PBrrtrn-sistemas-distribuidos-tp2-98446/blob/master/.img/avg_duration_with_precipitations_trip_ingestion_sequence.png)

## Known Issues
- El uso de nodos que centralizan el calculo de las queries aumentan el número de RTTs necesarios para distribuir los datos, además de limitar cuanto se puede escalar levantando nodos adicionales
- Los nodos DistanceCalculator no pueden escalar porque MontrealStationsOver6KmAvgTripDistanceIngestor esperan a terminar los llamados de RPC antes de encolar el próximo pedido, por lo que los pedidos siempre serán atendidos por el mismo nodo. Para escalar, se debería pasar el viaje al DistanceCalculator, este encontrar la distancia y hacer forwarding al nodo DistanceRunningAvg.