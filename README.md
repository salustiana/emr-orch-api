# BI EMR Clusters Manager

Esta aplicación gestiona las interacciones con EMR de manera que podamos:
* ejecutar steps en EMR
* levantar clusters y asignarles steps para que corran de manera secuencial
* optimizar la asignación de steps a clusters para reducir costos y tiempo de espera
* colectar métricas de uso del servicio

## Aclaración inicial

La documentación de esta aplicación está **WIP**. Si sentís que estaría bueno agregar algo puntual, hacenos un issue. Vamos a documentar antes lo que sea considerado más importante.


## SDK

La aplicación cuenta con un SDK en Python para la interacción con el servicio. Recomendamos su uso por sobre el de los endpoints HTTP. 

* Python SDK: [Rayuela](https://github.com/mercadolibre/fury_python-bi-cluster-manager)

Quien quiera agregar un SDK en otra tecnología, siéntase libre de contactarnos para facilitar la integración. 

## Endpoints

La aplicación cuenta con sus endpoints documentados usando [Swagger](https://swagger.io/docs/). De allí deberíamos poder sacar toda la información que necesitemos.

> La API no fue diseñada para su interacción con usuarios. Puede haber mensajes de error no descriptivos de cara al usuario. El uso del SDK es alentado.
