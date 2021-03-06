<?php

namespace Jncinet\Flysystem\Wcs\Sdk;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\RequestException;
use Psr\Http\Message\ResponseInterface;

class Support
{
    public static function multipartPost($url,
        $fields,
        $fileName,
        $mimeType = null,
                                         array $headers = [])
    {
        $headers = array_merge([
            'Host' => parse_url($url, PHP_URL_HOST),
            'User-Agent' => self::userAgent(),
            'Accept' => '*/*',
        ], $headers);
        $finalMimeType = empty($mimeType) ? 'application/octet-stream' : $mimeType;
        $finalFileName = self::escapeQuotes($fileName);
        $data = [];
        foreach ($fields as $key => $field) {
            if ($key == 'file') {
                $data[] = [
                    'name' => $key,
                    'contents' => $field,
                    'filename' => $finalFileName,
                    'headers' => ['Content-Type' => $finalMimeType]
                ];
            } else {
                $data[] = ['name' => $key, 'contents' => $field];
            }
        }
        try {
            $http = new Client();
            $response = $http->request('POST', $url, [
                'headers' => $headers,
                'multipart' => $data
            ]);
            if ($response->getReasonPhrase() === 'OK') {
                return json_decode((string)$response->getBody(), true);
            }
            return array();
        } catch (RequestException $e) {
            return $e;
        }
    }

    public static function get($url, $headers)
    {
        $headers = array_merge([
            'Host' => parse_url($url, PHP_URL_HOST),
            'User-Agent' => self::userAgent(),
        ], $headers);
        try {
            $http = new Client();
            $response = $http->get($url, ['headers' => $headers]);
            if ($response->getReasonPhrase() === 'OK') {
                return json_decode((string)$response->getBody(), true);
            }
            return array();
        } catch (RequestException $e) {
            return $e;
        }
    }

    public static function post($url, $body, $headers)
    {
        $headers = array_merge([
            'Host' => parse_url($url, PHP_URL_HOST),
            'User-Agent' => self::userAgent(),
        ], $headers);
        if (is_array($body)) {
            $body = http_build_query($body);
        }
        try {
            $http = new Client();
            $response = $http->post($url, ['headers' => $headers, 'data' => $body]);
            if ($response->getReasonPhrase() === 'OK') {
                return json_decode((string)$response->getBody(), true);
            }
            return array();
        } catch (RequestException $e) {
            return $e;
        }
    }

    /**
     * @return string
     */
    public static function userAgent(): string
    {
        $sdkInfo = 'WCS PHP SDK /' . Config::SDK_VER . ' (http://wcs.chinanetcenter.com/)';

        $systemInfo = php_uname('s');
        $machineInfo = php_uname('m');

        $envInfo = "($systemInfo/$machineInfo)";

        $phpVer = phpversion();

        return "$sdkInfo $envInfo PHP/$phpVer";
    }

    /**
     * ????????????????????????url safe???base64?????????
     *
     * @param string $data ???????????????????????????????????????
     *
     * @return string ?????????????????????
     */
    public static function base64_urlSafeEncode(string $data): string
    {
        $find = array('+', '/');
        $replace = array('-', '_');
        return str_replace($find, $replace, base64_encode($data));
    }

    /**
     * ????????????url safe???base64???????????????????????????
     *
     * @param string $str ???????????????????????????????????????
     *
     * @return string ?????????????????????
     */
    public static function base64_urlSafeDecode(string $str): string
    {
        $find = array('-', '_');
        $replace = array('+', '/');
        return base64_decode(str_replace($find, $replace, $str));
    }

    /**
     * ??????API??????????????????
     *
     * @param string $bucket ?????????????????????
     * @param string $key ?????????????????????
     *
     * @return string  ??????API?????????????????????
     */
    public static function entry(string $bucket, string $key): string
    {
        $en = $bucket;
        if (!empty($key)) {
            $en = $bucket . ':' . $key;
        }
        return self::base64_urlSafeEncode($en);
    }

    /**
     * ???????????????crc32?????????:
     *
     * @param $file string  ?????????????????????????????????
     *
     * @return string ???????????????crc32?????????
     */
    public static function crc32_file($file)
    {
        $hash = hash_file('crc32b', $file);
        $array = unpack('N', pack('H*', $hash));
        return sprintf('%u', $array[1]);
    }

    /**
     * ??????????????????crc32?????????
     *
     * @param $data ??????????????????????????????
     *
     * @return string ??????????????????crc32?????????
     */
    public static function crc32_data($data)
    {
        $hash = hash('crc32b', $data);
        $array = unpack('N', pack('H*', $hash));
        return sprintf('%u', $array[1]);
    }

    /**
     * array ???????????????????????????set
     *
     * @param array $array ?????????array
     * @param string $key key
     * @param string $value value ???null??? ?????????
     *
     * @return array ?????????array?????????????????????
     */
    public static function setWithoutEmpty(array &$array, string $key, string $value): array
    {
        if (!empty($value)) {
            $array[$key] = $value;
        }
        return $array;
    }

    /**
     * ????????????
     *
     * @param ResponseInterface $response
     * @return array
     */
    public static function response(ResponseInterface $response): array
    {
        if ($response->getReasonPhrase() === 'OK') {
            return json_decode((string)$response->getBody(), true);
        }
        return array();
    }

    /**
     *  ???up token??????accessKey???bucket
     *
     * @param $upToken
     * @return array(ak,bucket,err=null)
     */
    public static function explodeUpToken($upToken)
    {
        $items = explode(':', $upToken);
        if (count($items) != 3) {
            return array(null, null, "invalid uptoken");
        }
        $accessKey = $items[0];
        $putPolicy = json_decode(self::base64_urlSafeDecode($items[2]));
        $scope = $putPolicy->scope;
        $scopeItems = explode(':', $scope);
        $bucket = $scopeItems[0];
        return array($accessKey, $bucket, null);
    }

    private static function escapeQuotes($str)
    {
        $find = array("\\", "\"");
        $replace = array("\\\\", "\\\"");
        return str_replace($find, $replace, $str);
    }

    public static function getFileSize($filePath)
    {
        $size = filesize($filePath);
        if ($size < 0) {
            $return = self::sizeCurl($filePath);
            if ($return) {
                return $return;
            }

            $return = self::sizeNativeSeek($filePath);
            if ($return) {
                return $return;
            }

            $return = self::sizeCom($filePath);
            if ($return) {
                return $return;
            }

            $return = self::sizeExec($filePath);
            if ($return) {
                return $return;
            }

            throw new \ErrorException("Can not size of file $filePath!");
        }

        return $size;
    }

    protected static function sizeCurl($path)
    {
        // curl solution - cross platform and really cool :)
        if (function_exists("curl_init")) {
            $ch = curl_init("file://" . $path);
            curl_setopt($ch, CURLOPT_NOBODY, true);
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
            curl_setopt($ch, CURLOPT_HEADER, true);
            $data = curl_exec($ch);
            curl_close($ch);
            if ($data !== false && preg_match('/Content-Length: (\d+)/', $data, $matches)) {
                return (string)$matches[1];
            }
        } else {
            return false;
        }
    }

    protected static function sizeNativeSeek($path)
    {
        // This should work for large files on 64bit platforms and for small files every where
        $fp = fopen($path, "rb");
        if (!$fp) {
            return false;
        }
        flock($fp, LOCK_SH);
        $res = fseek($fp, 0, SEEK_END);
        if ($res === 0) {
            $pos = ftell($fp);
            flock($fp, LOCK_UN);
            fclose($fp);
            // $pos will be positive int if file is <2GB
            // if is >2GB <4GB it will be negative number
            if ($pos >= 0) {
                return (string)$pos;
            } else {
                return sprintf("%u", $pos);
            }
        } else {
            flock($fp, LOCK_UN);
            fclose($fp);
            return false;
        }
    }

    protected static function sizeCom($path)
    {
        if (class_exists("COM")) {
            // Use the Windows COM interface
            $fsobj = new COM('Scripting.FileSystemObject');
            if (dirname($path) == '.')
                $path = ((substr(getcwd(), -1) == DIRECTORY_SEPARATOR)
                    ? getcwd() . basename($path)
                    : getcwd() . DIRECTORY_SEPARATOR . basename($path));
            $f = $fsobj->GetFile($path);
            return (string)$f->Size;
        }
    }

    protected static function sizeExec($path)
    {
        // filesize using exec
        if (function_exists("exec")) {
            $escapedPath = escapeshellarg($path);

            if (strtoupper(substr(PHP_OS, 0, 3)) == 'WIN') { // Windows
                // Try using the NT substition modifier %~z
                $size = trim(exec("for %F in ($escapedPath) do @echo %~zF"));
            } else { // other OS
                // If the platform is not Windows, use the stat command (should work for *nix and MacOS)
                $size = trim(exec("stat -Lc%s $escapedPath"));
            }

            // If the return is not blank, not zero, and is number
            if (ctype_digit($size)) {
                return (string)$size;
            }
        }
        return false;
    }
}
