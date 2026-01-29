data-collection-service
Generated skeleton project (Java 17, Spring Boot).
Run:
  mvn -U clean package
  java -jar target/data-collection-service-0.0.1-SNAPSHOT.jar

## Docker 部署

项目已经内置 JDK 17 运行时镜像与 `docker-compose.yml`，可按以下步骤完成容器化部署。

### 1. 构建可运行的 JAR

```
mvn -U clean package -DskipTests
```

构建结果位于 `target/` 目录，Dockerfile 会把 JAR 复制到容器内的 `/app.jar`。

### 2. 构建镜像

```
docker build \
  --build-arg JAR_FILE=target/data-collection-service-0.0.1-SNAPSHOT.jar \
  -t data-collection-service:latest .
```

- 基础镜像：`eclipse-temurin:17-jdk-jammy`
- 入口：`java -jar /app.jar`
- 可通过 `--build-arg JAR_FILE=...` 指向不同版本的可执行包。

### 3. 单容器运行

```
docker run -d --name data-collection-service \
  -p 8080:8080 \
  -e SPRING_PROFILES_ACTIVE=prod \
  data-collection-service:latest
```

需要自定义端口或配置时，可增添 `-e SERVER_PORT=9090`、`-v /opt/logs:/logs` 等参数，并在应用配置里引用。

### 4. 使用 Docker Compose

仓库自带的 `docker-compose.yml` 会同时拉起应用和 Redis：

```
version: "3.9"
services:
  app:
    build: .
    image: data-collection-service:latest
    ports:
      - "8080:8080"
    environment:
      SPRING_PROFILES_ACTIVE: prod
      JAVA_OPTS: -Xms256m -Xmx512m
    volumes:
      - app-logs:/opt/app/logs
    depends_on:
      redis:
        condition: service_healthy
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5
volumes:
  app-logs:
  redis-data:
```

常用命令：

```
docker compose up -d

docker compose logs -f

docker compose down
```

如需持久化 Redis 数据或挂载应用日志，只需在相应服务下添加 `volumes` 映射，例如：

```
services:
  app:
    volumes:
      - ./logs:/logs
  redis:
    volumes:
      - redis-data:/data
volumes:
  redis-data:
```


- 鏌ヨ鎸囧畾璁惧鐨勬寚瀹氭暟鎹偣瀹炴椂鍊?锛?

```
GET聽/api/data/device/{deviceId}/point/
{pointId}
```
绀轰緥锛?GET /api/data/device/device-001/point/temperature
- 鏌ヨ鎸囧畾璁惧鐨勬墍鏈夋暟鎹偣瀹炴椂鍊?锛?

```
GET聽/api/data/device/{deviceId}
```
鏀寔鍙€夊弬鏁?pointIds 绛涢€夌壒瀹氭暟鎹偣锛?

```
GET聽/api/data/device/device-001?
pointIds=temperature,humidity
```
- 鏌ヨ鎵€鏈夎澶囧熀鏈俊鎭?锛?

```
GET聽/api/data/devices
```
- 鏌ヨ鎸囧畾璁惧鐨勬墍鏈夋暟鎹偣閰嶇疆 锛?

```
GET聽/api/data/device/{deviceId}/points
```



- 鍦?DataPoint 澧炲姞涓婃姤/鍙樺寲/浜嬩欢閰嶇疆瑙ｆ瀽锛坰rc/main/java/com/wangbin/collector/common/domain/entity/DataPoint.java:344 璧凤級锛屾敮鎸侀€氳繃 additionalConfig 閰嶇疆 reportEnabled銆乧hangeThreshold銆乧hangeMinIntervalMs銆乪ventEnabled銆?  
  eventMinIntervalMs锛屽苟缁х画閬靛惊鈥滄棤鍒悕=鍙繚鐣?raw鈥濈殑绾︽潫銆?
- ReportProperties 鏂板璋冨害涓庨檺娴佸弬鏁帮紙src/main/java/com/wangbin/collector/core/report/config/ReportProperties.java:56 璧凤級锛屽寘鎷粯璁?10s 鍛ㄦ湡銆佹渶灏忓彉鍖栬Е鍙戦棿闅斻€佷簨浠堕粯璁ら棿闅斻€佺綉鍏?MPS銆佸崟鍖呭瓧娈?瀛楄妭闄愬埗涓?schemaVersion锛屼究浜?
  缁熶竴璋冧紭銆?
- DeviceShadow/ShadowManager 鎵╁睍涓哄畬鏁寸殑瑙﹀彂寮曟搸锛氬奖瀛愪繚瀛樻渶鍚庝笂鎶ュ€?鍙樺寲/浜嬩欢鏃堕棿锛坰rc/main/java/com/wangbin/collector/core/report/shadow/DeviceShadow.java:17-107锛夛紝ShadowManager 鍦?apply 涓悓鏃惰绠椻€滃彉鍖栬Е鍙?+ 浜嬩欢瑙﹀彂鈥濆苟杩?
  鍥?ShadowUpdateResult锛坰rc/main/java/com/wangbin/collector/core/report/shadow/ShadowManager.java:31锛夛紱浜嬩欢鏀寔 AlarmRule銆乵etadata 鏍囪鍙婅川閲忛€€鍖栦笁绉嶆潵婧愶紝骞跺鐢ㄦ渶灏忛棿闅旓紙ShadowManager.java:90 璧凤級銆?
- CacheReportService 褰诲簳閲嶅啓锛氬畾鏃跺揩鐓?+ 鍙樺寲绔嬪嵆鍒锋柊 + 浜嬩欢鍗虫椂涓婃姤锛屽苟闄勫甫缃戝叧闄愭祦锛堝揩鐓у彈闄愩€佷簨浠朵笉鍙楅檺锛夛紝鍚屾椂鍦ㄦ墍鏈?chunk 涓婃姤鍚庡洖鍐欏奖瀛愮姸鎬侊紙src/main/java/com/wangbin/collector/core/report/service/                      
  CacheReportService.java:1-309锛夈€備簨浠朵笂鎶ヤ娇鐢?MessageConstant.MESSAGE_TYPE_EVENT_POST锛屽揩鐓?浜嬩欢閮芥墦涓?schemaVersion銆乻eq銆乥atchId/chunk*锛岀鍚?200 瀛楁涓?128KB 鍙岄槇鍊笺€?
- 浜嬩欢/鍙樺寲榛樿琛屼负璇存槑锛氬彉鍖栬Е鍙戜粎鍦ㄧ偣浣嶉厤缃?changeThreshold 鏃剁敓鏁堬紱浜嬩欢鐢?alarmRule銆乵etadata.event*銆佹垨璐ㄩ噺璺岃嚦 WARNING 浠ヤ笅瑙﹀彂锛屽苟鍙€?eventMinIntervalMs 闄愬埗棰戠巼銆?

閰嶇疆涓庝娇鐢ㄨ鐐?

- 瑕佸惎鐢ㄦā寮?B锛堝彉鍖栬Е鍙戯級锛屼负鐐逛綅 additionalConfig 璁剧疆锛歿"changeThreshold":5.0,"changeMinIntervalMs":3000} 绛夛紱妯″紡 C锛堜簨浠讹級鍙部鐢?alarmRule锛屾垨鍦ㄥ鐞嗛摼涓啓鍏?metadata.eventTriggered=true/eventType銆?
- 鍏ㄥ眬涓婃姤鑺傚鍙傛暟闆嗕腑鍦?collector.report.*锛氶粯璁?interval=10000銆乵inReportIntervalMs=2000銆乪ventMinIntervalMs=5000銆乵axGatewayMessagesPerSecond=200銆乵axPropertiesPerMessage=200銆乵axPayloadBytes=131072锛屽彲鎸夎澶囪妯¤皟浼樸€?
- 浠嶉渶纭繚鐐逛綅鏈?pointAlias 鎵嶈兘杩涜澶囧揩鐓э紱鍚﹀垯鍙繚鐣?raw锛堜絾浜嬩欢浠嶅彲瑙﹀彂锛屼笂鎶ュ瓧娈靛彇 pointCode/pointId锛夈€?

娴嬭瘯

- mvn -q -DskipTests "-Dspring-boot.repackage.skip=true" package

鑻ュ悗缁渶鏇村鍛婅鍏冧俊鎭垨浜戠 schema锛屽搴斿彲鍦?ShadowManager.EventInfo 鍜?CacheReportService.dispatchEvent 涓墿灞曘€?



## 浼樺寲

- DataPoint 鐜板湪鐩存帴瑙ｆ瀽 reportEnabled/changeThreshold/changeMinIntervalMs/eventEnabled/eventMinIntervalMs锛屽苟缁х画浠?pointAlias 浣滀负涓婃姤瀛楁鍒ゅ畾锛屼繚璇佸湪閰嶇疆灞傚氨鑳界瓫閫夆€滃叆蹇収/鍙樺寲/浜嬩欢鈥濈偣锛坰rc/main/java/com/wangbin/collector/  
  common/domain/entity/DataPoint.java:344锛夈€?
  - ReportData 澧炲己涓哄睘鎬ф槧灏?+ 鏃堕棿鎴冲弻 Map锛屽苟鍦?addProperty 鏃堕槻姝㈤噸澶嶅瓧娈碉紱鏂板 Logger 鍚庤嫢閲嶅瀛楁浼氳褰曞苟璺宠繃锛屽悓鏃?chunk 浠嶆惡甯?propertyTs銆乹uality銆乥atchId/seq/chunk* 鍏冩暟鎹紙src/main/java/com/wangbin/collector/core/    
    report/model/ReportData.java:20-198锛夈€?
  - 褰卞瓙灞傞噸鏋勶細DeviceShadow 缁存姢鏈€鏂板€笺€佷笂娆′笂鎶ユ椂闂存埑銆佸彉鍖?浜嬩欢瑙﹀彂鏃堕棿鍙婁簨浠剁鍚嶏紱ShadowManager 鍦?apply 涓悓姝ヨ绠椻€滃彉鍖栬Е鍙?+ 浜嬩欢瑙﹀彂鈥濓紝浜嬩欢鍘婚噸鎸?eventType + message hash 涓斿彈 eventMinIntervalMs 鎺у埗锛屾弧瓒斥€滃悓绫诲瀷浜嬩欢绐楀彛
    鍐呮渶澶?1 娆♀€濈殑绾︽潫锛坰rc/main/java/com/wangbin/collector/core/report/shadow 鍖咃級銆?
  - CacheReportService 瀹炵幇涓夌妯″紡骞跺姞鍏ラ檺娴?鍘婚噸/鎸夊瓧娈靛洖鍐欙細瀹氭椂蹇収锛堝垏鍖呭悗寮傛涓婃姤锛夈€佸彉鍖栬Е鍙戠珛鍗冲埛銆佷簨浠惰Е鍙戝嵆鏃跺崟鐙彂閫侊紱chunk 涓婃姤閲囩敤 FlushTracker 璺熻釜鎴愬姛/澶辫触锛屽彧鏈夋垚鍔熺殑瀛楁鎵嶅洖鍐欙紝澶辫触瀛楁淇濈暀绛夊緟涓嬩竴杞紝涓旀暣鍖呬粎鍦?
    鎵€鏈?chunk 鎴愬姛鏃舵墠娓呰剰锛涘揩鐓т笂鎶ュ彈 maxPropertiesPerMessage銆乵axPayloadBytes銆乵axGatewayMessagesPerSecond銆乵inReportIntervalMs 绾︽潫锛屼簨浠堕€氶亾涓嶅彈闄愶紙src/main/java/com/wangbin/collector/core/report/service/                   
    CacheReportService.java:1-330锛夈€?
  - 浜嬩欢涓婃姤浣跨敤 MessageConstant.MESSAGE_TYPE_EVENT_POST锛屽苟鎼哄甫瑙勫垯 ID/鍚嶇О銆佺瓑绾с€佸埆鍚嶇瓑涓婁笅鏂囷紱鍙樺寲/浜嬩欢鐨勬渶灏忛棿闅斿彲 per-point 閰嶇疆锛岄粯璁ゅ€兼潵鑷?collector.report.*銆俁eportProperties 鏂板 minReportIntervalMs銆?                 
    eventMinIntervalMs銆乵axGatewayMessagesPerSecond銆乵axPropertiesPerMessage銆乵axPayloadBytes銆乻chemaVersion 绛夊紑绠卞弬鏁帮紙src/main/java/com/wangbin/collector/core/report/config/ReportProperties.java:56-86锛夈€?

  娴嬭瘯

  - mvn -q -DskipTests "-Dspring-boot.repackage.skip=true" package

  濡傞渶鍚庣画鎷撳睍锛堜緥濡傛洿澶氫簨浠跺瓧娈点€佸け璐ラ噸璇曠瓥鐣ユ垨浜戠鍥炴墽锛夊彲鍦?FlushTracker/dispatchEvent 鍩虹涓婄户缁寮恒€?



##  浼樺寲 
鈥?褰撳墠瀹炵幇杩樼己灏?涓嶆弧瓒崇殑鐐癸細

- DataPoint 浠嶆棫浠?鈥滃彧瑕?alias 闈炵┖灏遍粯璁?reportEnabled=true鈥濓紝娌℃湁钀藉疄鈥渞eportEnabled 榛樿 false銆佸繀椤绘樉寮忓紑鍚€濅互鍙?reportField 瀛楁銆備篃娌℃湁澶勭悊鈥滄棤鍒悕鍗?raw-only鈥濅箣澶栫殑鈥渞eportEnabled=false 鏃跺嵆渚挎湁鍒悕涔熶笉杩涘揩鐓р€濈殑閫昏緫銆?
- FieldUniquenessValidator 灏氭湭鎺ュ叆/鏈 reportField 鍐茬獊鍋氶檷绾у鐞嗭紝ReportData.addProperty() 铏介槻姝㈣鐩栵紝浣嗛亣鍒板啿绐佸彧鏄棩蹇?warn锛屾病鏈夊皢鍐茬獊鐐归檷绾т负 raw-only銆?
- 鍒囧寘 splitSnapshot 铏藉悓姝ュ瓧娈碉紝浣嗘病鏈夊湪鍙戦€佸け璐ユ椂淇濈暀 chunk 浠ヤ究涓嬩竴杞噸璇曪紱flush 鍥炲啓鍙熀浜庢垚鍔?chunk锛屼絾澶辫触 chunk 娌℃湁澶嶄綅 shadow 鐘舵€佹垨瑙﹀彂涓嬫鍙戦€併€?
- 浜嬩欢闃查鏆翠粎閫氳繃 eventType+message 鍒ゆ柇锛屾湭瀹炵幇鈥滀簨浠跺唴瀹?hash 鍘婚噸 + eventMinIntervalMs 鍐呮渶澶?1 娆♀€濆畬鏁磋鍒欙紝涔熸湭灏?eventMinIntervalMs 搴旂敤浜?metadata 椹卞姩鐨勪簨浠躲€?
- 涓婅堪鍔熻兘缂轰箯鍗曞厓娴嬭瘯锛涜姹傜殑 DataPoint/reportField銆佸彉鍖栬Е鍙戙€佷簨浠惰Е鍙戙€侀槻椋庢毚銆佸垏鍖呭悓姝ャ€佸瓧娈靛啿绐佺瓑娴嬭瘯灏氭湭缂栧啓銆?




## 浼樺寲 
- src/main/java/com/wangbin/collector/common/domain/entity/DataPoint.java:376-426 鐜伴泦涓В鏋?reportEnabled/reportField/change/event 閰嶇疆锛実etReportField() 鍏堝彇鏄惧紡瀛楁鍐嶅洖閫€鍒板埆鍚嶏紝鏃犲埆鍚嶅垯寮哄埗 raw-only锛沬sReportEnabled() 鍙湁
  鍦ㄦ爣璁颁负 true 涓斿瓧娈典笉鍐茬獊鏃舵墠杩斿洖 true銆?
  - src/main/java/com/wangbin/collector/core/report/config/ReportProperties.java:36-88 琛ラ綈璋冨害涓庨檺娴佸弬鏁帮紙intervalMs銆乵inReportIntervalMs銆乵axGatewayMessagesPerSecond銆乵axPropertiesPerMessage銆乵axPayloadBytes銆乻chemaVersion锛夛紝
    渚涜仛鍚?闄愭祦閫昏緫缁熶竴璇诲彇銆?
  - src/main/java/com/wangbin/collector/core/report/service/CacheReportService.java:204-333,267-322,489-522 閲嶅啓鍒囧寘涓庡彂閫佹祦绋嬶細splitSnapshot 鎭掑姞 chunk 鍏冩暟鎹紝payload/瀛楁鍙岄槇鍊煎垏鍒嗭紱鍙戦€佸墠鐧昏 chunk key锛宧andleChunkResult 澶?
    璐ユ寜璁惧閲嶈瘯骞朵粎瀵规垚鍔?chunk 鍥炲啓褰卞瓙锛汧lushTracker 璺熻釜 in-flight 鏁板拰姣?chunk 鐨勯噸璇曟鏁般€?
  - src/main/java/com/wangbin/collector/core/report/validator/FieldUniquenessValidator.java:18-41/src/main/java/com/wangbin/collector/core/report/shadow/ShadowManager.java:48-146 淇濊瘉璁惧鍐?reportField 鍞竴骞跺湪褰卞瓙灞傚疄鐜板彉鍖栭槇鍊?
    涓庝簨浠堕槻椋庢毚锛堟渶灏忛棿闅?+ eventType+鍐呭鎸囩汗鍘婚噸锛夈€?
  - 鏂板鍗曞厓娴嬭瘯瑕嗙洊鍏抽敭璺緞锛欴ataPointTest 楠岃瘉瀛楁瑙ｆ瀽(src/test/java/com/wangbin/collector/common/domain/entity/DataPointTest.java:13)銆丗ieldUniquenessValidatorTest 楠岃瘉鍐茬獊闄嶇骇(鈥?validator/                                    
    FieldUniquenessValidatorTest.java:26)銆丼hadowManagerTest 瑕嗙洊鍙樺寲瑙﹀彂涓庝簨浠惰妭娴?鈥?shadow/ShadowManagerTest.java:27,59)銆丆acheReportServiceTest 鏍￠獙鍒囧寘鏃跺睘鎬?璐ㄩ噺/鏃堕棿鎴冲悓姝?鈥?service/CacheReportServiceTest.java:15)銆?

  娴嬭瘯

  - mvn -q test

  鏈畬鎴?娉ㄦ剰

  - CacheReportService.java:204-248 涓?maxPayloadBytes 浠嶆寜瀛楁鍚?鍊煎瓧绗︿覆闀垮害绮椾及 MQTT 杞借嵎銆傚鏋滈渶瑕佷弗鏍肩殑 UTF-8 JSON 瀛楄妭绾ф牎楠岋紝鍚庣画闇€鏇挎崲涓虹湡瀹炲簭鍒楀寲娴嬬畻銆?

  鍚庣画鍙寜闇€鍦ㄥ疄闄呯偣浣?additionalConfig 涓ˉ榻?reportEnabled/reportField锛屽苟瑙傚療 128KB 闄愬埗鏄惁闇€瑕佹洿绮剧‘鐨?payload 璁＄畻銆?      



## 涓婃姤閰嶇疆
鈥?- 浠ｇ爜鐜板凡鍏峰璁惧绾ц仛鍚?鍙樺寲/浜嬩欢涓夌被涓婃姤銆佸瓧娈靛敮涓€鏍￠獙銆佸垏鍖呬笌闄愭祦绛夐€昏緫锛屼絾榛樿渚濊禆 ReportProperties 涓庣偣浣?additionalConfig銆傝嫢 collector.report.enabled=true 涓?MQTT 閰嶇疆姝ｇ‘锛屽鍣ㄥ惎鍔ㄥ悗鍗冲彲鎸?10s 鍛ㄦ湡鍙戝揩鐓э紙鍙樺寲/浜嬩欢鎸夎
鍒欒Е鍙戯級锛屾棤闇€鍐嶅崟鐐瑰疄鏃舵帹閫併€?
- 鍚敤姝ラ锛氣憼 鍦ㄧ偣浣嶉厤缃噷璁剧疆 additionalConfig锛岃嚦灏戝寘鍚?reportEnabled=true 涓?pointAlias锛堟垨鏄惧紡 reportField锛夛紝缂哄皯鍒悕鐨勭偣浠嶅彧鍐?raw 鏁版嵁锛涒憽 鎸夐渶璁剧疆 changeThreshold銆乧hangeMinIntervalMs銆乪ventEnabled 绛夊弬鏁帮紱鈶?鍦?        
  application.yml 涓～濂?collector.report.mqtt.*锛坆rokerUrl/clientId/topic/qos 绛夛級锛涒懀 闇€瑕佽皟鏁村懆鏈?闄愭祦鍙湪鍚屼竴閰嶇疆椤逛慨鏀?intervalMs銆乵axPropertiesPerMessage銆乵axPayloadBytes 绛夈€?
- 閰嶇疆瀹屾垚鍚庨噸鍚綉鍏冲嵆鍙紑濮嬫眹鎶ャ€傝嫢瑕侀獙璇侊紝鍙瀵熸棩蹇椾腑 CacheReportService/MqttReportHandler 鏄惁杈撳嚭鍙戦€佽褰曘€?



## 涓婃姤闂
shadowManager.apply(...) 浼氬湪涓夌鎯呭喌涓嬭繑鍥?eventInfo锛?

1. 鐐逛綅鑷韩鍛婅瑙勫垯鍛戒腑锛欴ataPoint.alarmRule 瑕佹湁鍚敤鐨勮鍒欙紝涓?processResult.getFinalValue() 瑙﹀彂浜?rule.checkAlarm()銆?
2. ProcessResult.metadata 涓诲姩鏍囪浜嬩欢锛氶噰闆嗘垨澶勭悊閾捐矾鎶?metadata.put("eventTriggered", true)锛堟垨鑷冲皯 metadata.put("eventType", "...")锛夊啓杩涘幓锛屽苟闄勫甫 eventLevel/eventMessage 绛変俊鎭€?
3. 璐ㄩ噺闄嶇骇锛歱rocessResult.getQuality() 浣庝簬 QualityEnum.WARNING锛堜緥濡?BAD / NOT_CONNECTED 绛夛級锛屽氨浼氳嚜鍔ㄧ敓鎴?eventType=QUALITY 鐨勪簨浠躲€?

闄ゆ涔嬪锛岃繕瑕佹弧瓒充互涓嬬害鏉燂紝鍚﹀垯鍗充究瑙﹀彂鏉′欢鎴愮珛涔熶細杩囨护鎺夛細

- DataPoint.isEventReportingEnabled() 蹇呴』涓?true銆傞粯璁ゅ鏋?additionalConfig 娌℃湁 eventEnabled=false锛屽氨瑙嗕负寮€鍚紱浣嗘湁浜涚偣浣嶅鏋滈厤缃簡 eventEnabled=false 鎴?alarmEnabled=0锛屼簨浠朵細琚暣浣撳叧闂€?
- 鍚屼竴绫诲瀷浜嬩欢鍦?eventMinIntervalMs 鍐呭彧鍏佽鍑虹幇涓€娆°€傞粯璁?5000ms锛屽彲閫氳繃 additionalConfig.eventMinIntervalMs锛堟垨 ReportProperties.eventMinIntervalMs 鍏ㄥ眬榛樿锛夎皟鏁淬€傚鏋滀綘棰戠箒妯℃嫙鍚屼竴浜嬩欢锛屽彲鑳藉洜涓鸿妭娴?鍘婚噸琚拷鐣ャ€?
- 浜嬩欢鎸囩汗鍘婚噸锛歟ventType + message/ruleId/ruleName 鍦?eventMinIntervalMs 鍐呴噸澶嶅嚭鐜颁篃浼氳鍒ゅ畾涓洪噸澶嶃€?

鍥犳锛屽鏋滀綘姣忔閮芥嬁涓嶅埌 eventInfo锛岃妫€鏌ワ細

1. 鐐逛綅鐨?additionalConfig 鏄惁鍖呭惈 eventEnabled=false 鎴?eventMinIntervalMs 杩囧ぇ锛?
2. 褰撳墠 ProcessResult 鏈夋病鏈?metadata 鏍囪鎴栬川閲忛檷绾э紱
3. 鏄惁鍦ㄤ笂涓€鏉¤Е鍙戝悗灏氭湭瓒呰繃 eventMinIntervalMs锛堝彲浠ユ殏鏃舵妸 interval 璁句负寰堝皬楠岃瘉锛夛紱
4. 鏃ュ織涓槸鍚﹀嚭鐜?FieldUniquenessValidator 鐨勫啿绐佹垨鍏朵粬闄嶇骇鎻愮ず鈥斺€旇闄嶇骇鐨勭偣铏界劧杩樿兘閲囬泦锛屼絾褰卞瓙閲屼笉浼氬仛鍙樺寲/浜嬩欢鍒ゆ柇銆?

濡傛灉浣犻渶瑕侀€氳繃浠ｇ爜涓诲姩瑙﹀彂浜嬩欢锛屽彲浠ュ湪閲囬泦缁撴灉閲屽姞涓婏細

ProcessResult result = new ProcessResult();
result.addMetadata("eventTriggered", true);
result.addMetadata("eventType", "CUSTOM_EVENT");
result.addMetadata("eventLevel", "WARNING");
result.addMetadata("eventMessage", "test");



## 浼樺寲
1. 鍛婅锛氱淮鎸佸師鏈?DataPoint.alarmEnabled=1 涓?AlarmRule 閰嶇疆鍗冲彲锛涘綋鍛婅鍙戠敓鏃讹紝AlertManager.getRecentAlerts() 浼氳繑鍥炶Е鍙戣褰曪紝涓婃父渚濇棫鏀跺埌 CacheReportService 鍙戝嚭鐨勪簨浠朵笂鎶ャ€?
2. 鍗曚綅锛氳嫢閲囬泦鍊肩殑鍘熷鍗曚綅涓?DataPoint.unit 涓嶅悓锛屽彲鍦?ProcessContext 鍐欏叆 context.addAttribute("rawUnit", "掳F") 鎴栧湪鏁版嵁鐐圭殑 additionalConfig.sourceUnit 涓０鏄庯紱蹇呰鏃跺彲鍦ㄩ厤缃噷瑕嗙洊 contextUnitAttribute 鍜?                  
   additionalConfigUnitKey銆?
3. 姝诲尯缂撳瓨锛氶€氳繃澶勭悊鍣ㄩ厤缃腑鐨?sampleTtlMs銆乵axCacheSize 鎺у埗缂撳瓨鐢熷懡鏈熴€?



## 浼樺寲
鈥?- MQTT 鍥炴墽涓庣绾块噸浼?鈥?鐜板湪鍙互鍦?collector.report.mqtt 涓厤缃?ack-topic-prefix/ack-topic-suffix锛堟垨鐩存帴瑕嗗啓妯℃澘锛夛紝鎵€鏈変笂鎶ヤ細鑷姩璁㈤槄浜戠鍥炴墽 Topic锛屽苟鎸?ACK JSON 閲岀殑 id/code/msg 鍏宠仈缁撴灉锛圡qttReportHandler 宸插鍔?ACK 澶勭悊
鍣級銆傚綋瀹㈡埛绔帀绾挎垨鍥炴墽瓒呮椂锛孋acheReportService 浼氭妸缁撴灉鏍囪涓?deferred 骞跺欢鏃堕噸璇曪紝绛夎繛鎺ユ仮澶嶅啀琛ュ彂锛涙棩蹇楅噷浼氱湅鍒?鈥淒eferred retry scheduled 鈥︹€?鏂逛究纭銆?
- 鍛婅閾捐矾鎵撻€?鈥?AlertNotification 鏂板璁惧鍚嶇О銆佸崟浣嶇瓑瀛楁锛孌ataQualityProcessor 瑙﹀彂鍛婅鏃朵細濉ソ杩欎簺淇℃伅鍚庝氦缁?AlertManager銆侫lertManager 涓嶅啀鍙墦鍗版棩蹇楋紝鑰屾槸璋冪敤鏂板姞鐨?CacheReportService.reportAlert 鐩存帴涓婂彂浜嬩欢銆?         
  ShadowManager 鍦ㄥ尮閰嶅憡璀﹁鍒欏墠浼氭鏌?DataPoint.alarmEnabled==1锛岄伩鍏嶈瑙﹀彂銆?
- 閰嶇疆涓庢枃妗?鈥?application.yml 鍜?README.md 閮芥洿鏂颁簡鍏充簬 ACK Topic 鐨勮鏄庯紝鐓х潃濉笂浜戠瑕佹眰鐨勫墠缂€/鍚庣紑鍗冲彲浣跨敤锛涘闇€瑕嗙洊榛樿妯℃澘鍙洿鎺ュ啓瀹屾暣鐨?ackTopicTemplate銆?

浣跨敤姝ラ锛堝杩帮級锛?

1. 鍦?application.yml锛堟垨鐩稿簲鐜閰嶇疆锛夐噷閰嶇疆濂?MQTT 璐﹀彿銆佷笂鎶?Topic 浠ュ強 ack-topic-prefix/suffix锛屽惎鍔ㄥ悗绯荤粺浼氳嚜鍔ㄨ闃?/sys/<projectKey>/<deviceName>/鈥?杩欑被鍥炴墽闃熷垪銆?
2. 甯屾湜涓婃姤鍛婅鐨勭偣浣嶈鍦?DataPoint 閰嶇疆閲屾妸 alarmEnabled 璁剧疆涓?1锛涙祦绋嬩細鑷姩鎶婂憡璀﹂€氳繃 reportAlert 涓婂彂锛屽苟闄勫甫 unit/deviceName 绛夊厓淇℃伅銆?
3. 濡傛灉 MQTT 鏂嚎锛岀郴缁熶細鑷姩灏嗘壒娆℃爣璁颁负 deferred 骞剁瓑寰呴噸杩烇紝鏃犻渶浜哄伐骞查銆
