@Service
public class GeneralService {

    private static final Logger log = LoggerFactory.getLogger(GeneralService.class);

    private final RedisTemplate<String, Object> redisTemplate;

    private final S3Helper s3Helper;

    private final ObjectMapper objectMapper;

    public GeneralService(RedisTemplate<String, Object> redisTemplate, S3Helper s3Helper, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.s3Helper = s3Helper;
        this.objectMapper = objectMapper;
    }

    public String createData(Map<String, Object> data){
        try {
            String stringData = objectMapper.writeValueAsString(data);
            s3Helper.saveObject(stringData);
            log.info("Data saved to S3");
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        return "Data saved successfully";
    }

    public Map<String, Object> getData(){
        if (isRedisPopulated()){
           return loadFromRedis();
        }
        Map<String, Object> value = loadDataFromS3();
        saveDataToRedis(value);
        return value;
    }

    public Map<String, Object> getDataByPath(String keyPath){
        String[] partPath = keyPath.split("\\.");
        if(partPath.length == 0) return null;
        Map<String, Object> data = getData();
        Object serviceData = data.get(partPath[0]);
        if(serviceData == null) return null;

        // Traverse the path to the find nested key
        for (int i=1; i < partPath.length; i++){
            if (serviceData instanceof Map){
                serviceData = ((Map<?,?>)serviceData).get(partPath[i]);
            }else{
                return null;
            }
        }
        return Map.of(keyPath, serviceData);
    }

    public String clearCache(){
        Set<String> keys = redisTemplate.keys("service_data::*");
        if (keys.isEmpty()){
            return "There is no data to be clear";
        }else{
            redisTemplate.delete(keys);
            return "Cache cleared successfully for service data";
        }
    }

    private void saveDataToRedis(Map<String, Object> value){
        value.forEach((serviceName, serviceData) ->{
            String key = generateKeyName(serviceName);
            redisTemplate.opsForValue().set(key, serviceData);
        });
    }

    private Map<String, Object> loadDataFromS3(){
        try {
            String s3Data = s3Helper.getObject();
            return objectMapper.readValue(s3Data, new TypeReference<>() {});
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private String generateKeyName(String serviceName){
        return "service_data::" + serviceName;
    }

    private boolean isRedisPopulated(){
        Set<String> keys = redisTemplate.keys("service_data::*");
        return !keys.isEmpty();
    }

    private Map<String, Object> loadFromRedis(){
        Set<String> keys = redisTemplate.keys("service_data::*");
        Map<String, Object> allData = new HashMap<>();
        if (!keys.isEmpty()) {
            for (String key : keys) {
                String serviceName = key.replace("service_data::", "");
                Object serviceData = redisTemplate.opsForValue().get(key);
                allData.put(serviceName, serviceData);
            }
        }
        return allData;
    }
}