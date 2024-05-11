<?php
// Убедитесь, что расширение php-yaml установлено и включено

// Загрузка содержимого файла конфигурации nvr.yaml
$nvrConfig = yaml_parse_file('nvr.yaml');

// Проверка наличия пути к файлу в конфигурации
if (isset($nvrConfig['go2rtc_config_path'])) {
    $go2rtc_config_path = $nvrConfig['go2rtc_config_path'];
    // Загрузка содержимого YAML файла по полученному пути
    $yamlContents = yaml_parse_file($go2rtc_config_path);

    // Проверка наличия раздела 'streams' и извлечение ключей
    if (isset($yamlContents['streams'])) {
        $streamsKeys = array_keys($yamlContents['streams']);
        // Кодирование ключей в JSON и вывод
        echo json_encode(['cameras' => $streamsKeys]);
    } else {
        echo "Раздел 'streams' не найден в YAML файле.";
    }
} else {
    echo "Путь к YAML файлу не найден в файле конфигурации nvr.yaml.";
}
?>
