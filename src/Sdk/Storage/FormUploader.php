<?php

namespace Jncinet\Flysystem\Wcs\Sdk\Storage;

use Jncinet\Flysystem\Wcs\Sdk\Config;
use Jncinet\Flysystem\Wcs\Sdk\Support;

final class FormUploader
{
    /**
     * 上传二进制流, 内部使用
     *
     * @param string $upToken 上传凭证
     * @param mixed $key 上传文件名
     * @param mixed $data 上传二进制流
     * @param Config $config 上传配置
     * @param mixed $params 自定义变量
     * @param mixed $mime 上传数据的mimeType
     * @param mixed $filename 原文件名
     *
     * @return mixed 测试时全返回null，但文件被创建成功了
     * @link https://wcs.chinanetcenter.com/document/API/FileUpload/Upload
     */
    public static function put(
        string $upToken,
               $key,
               $data,
        Config $config,
               $params,
               $mime,
               $filename
    )
    {
        $fields = array('token' => $upToken);
        if (!is_null($key)) {
            $fields['key'] = $key;
        }

        //enable crc32 check by default
        $fields['crc32'] = Support::crc32_data($data);

        if (is_array($params)) {
            foreach ($params as $k => $v) {
                $fields[$k] = $v;
            }
        }
        $fields['file'] = $data;
        $url = $config->getPutDomain() . '/file/upload';
        return Support::multipartPost($url, $fields, $filename, $mime);
    }

    /**
     * 上传文件，内部使用
     *
     * @param string $upToken 上传凭证
     * @param mixed $key 上传文件名
     * @param mixed $filePath 上传文件的路径
     * @param Config $config 上传配置
     * @param mixed $params 自定义变量，规格参考
     * @param mixed $mime 上传数据的mimeType
     *
     * @return mixed
     */
    public static function putFile(
        string $upToken,
               $key,
               $filePath,
        Config $config,
               $params,
               $mime
    )
    {
        $fields = array('token' => $upToken);
        if ($key !== null) {
            $fields['key'] = $key;
        }

        $fields['crc32'] = Support::crc32_file($filePath);

        if (is_array($params)) {
            foreach ($params as $k => $v) {
                $fields[$k] = $v;
            }
        }
        $fields['key'] = $key;
        $fields['file'] =  @fopen($filePath, 'r');

        $url = $config->getPutDomain() . '/file/upload';
        return Support::multipartPost($url, $fields, basename($filePath), $mime);
    }
}
