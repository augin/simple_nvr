<?php

$nvrConfig = yaml_parse_file('/config/nvr.yaml');
$base_dir = $nvrConfig['base_dir'];

if (is_dir("video")) {

   }
    else
    {
    symlink($base_dir, "video");
    }

// Function to recursively get all files in a directory
function getFiles($dir, &$visitedDirs = []) {
    $files = glob(rtrim($dir, '/') . '/*');
    $result = [];

    foreach ($files as $file) {
        if (is_dir($file) && !in_array($file, $visitedDirs)) {
            $visitedDirs[] = $file;
            $result = array_merge($result, getFiles($file, $visitedDirs));
        } else {
            $result[] = $file;
        }
    }

    return $result;
}

// Main function to get files and encode them in JSON format
function getAllFilesJSON($dir) {
    $visitedDirs = [];
    $files = getFiles($dir, $visitedDirs);
    $result = [];
    rsort($files);
    foreach ($files as $file) {
        $folder = dirname($file);
        $filename = basename($file);
        $result[$folder][] = $filename;
    }

    return json_encode($result);
}

//usage
$directory = 'video/'.$_GET['camera'];
$jsonData = getAllFilesJSON($directory);
echo $jsonData;
?>
