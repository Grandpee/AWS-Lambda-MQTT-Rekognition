import boto3
import time
import os
import logging
import uuid
import sys
import json

logger = logging.getLogger()

##############################################################
# lambda main function
##############################################################
def lambda_handler(event, context):
    logger.info("in lambda_handler!")
    #default setting
    regionName = 'us-east-1' #Region that all the service in.
    s3_member_bucket = 'member-image'
    s3_final_bucket = 'final-image'
    s3_record_bucket = 'record-image'
    member_table = 'member_sheet'
    _time = time.time()
    
    #intialize
    dynamoDB = boto3.client('dynamodb', region_name=regionName)
    s3_client = boto3.client('s3', region_name=regionName)
    s3 = boto3.resource('s3', region_name=regionName)
    iot = boto3.client('iot-data', region_name=regionName)
    rekognition = boto3.client('rekognition', region_name=regionName)
    
    imageName = time.asctime(time.localtime(_time)) + '.jpg'
    copy_image_to_s3(s3, s3_final_bucket, s3_record_bucket, imageName)

    response = dynamoDB.describe_table(TableName='memberSheet')
    memberCount = int(response['Table']['ItemCount'])
    #memberCount = 1

    memberID, similarity = compare_face(s3_member_bucket, s3_final_bucket, memberCount, rekognition)
    
    logger.info("pass compare_face!")

    if (memberID != -1 and similarity > 80.0):
        iot.publish(topic = 'knock/open/open', qos = 1, payload = 'True')
        updateDB(dynamoDB, memberID, _time)
    
    
##############################################################
# updateDB: update member's door knock log
##############################################################
def updateDB(dynamoDB, memberID, _time):
    response = dynamoDB.get_item(TableName='memberSheet', Key={'memberID': { 'S': '0'}})
    gender = str(response['Item']['gender']['S'])
    name = str(response['Item']['name']['S'])
    role = str(response['Item']['role']['S'])
    localTime = time.asctime(time.localtime(_time))

    dynamoDB.put_item(
            TableName = 'member_log',
            Item = {
                'time': { 'S': str(_time) },
                'name': { 'S': name },
                'gender': { 'S': gender },
                'role': { 'S': role },
                'localTime': {'S': localTime}
                }
            )



##################################################################################
# copy_image_to_s3: copy the photo taken by camera to photo storage bucket.
##################################################################################
def copy_image_to_s3(s3, sourceBucket, targetBucket, key):
    s3.Object(targetBucket, key).copy_from(CopySource = sourceBucket+'/public/'+'image3.jpg')
    object_acl = s3.ObjectAcl(targetBucket, key)
    object_acl.put(ACL='public-read')

    
################################################################################################################
# compare_face: Evoke aws facial recognition service to compare faces between member's photos and taken photo.
################################################################################################################
def compare_face(sourceBucket, targetBucket, memberCount, rekognition):
    threshold = 80.0
    for i in range(memberCount):
        try:
            response = rekognition.compare_faces(
                    SourceImage = {
                            'S3Object': {
                                    'Bucket': sourceBucket,
                                    'Name': 'image' + str(i) + '.jpg'
                                }
                        },
                    TargetImage = {
                            'S3Object': {
                                    'Bucket': targetBucket,
                                    'Name': 'public/image3.jpg'
                                }
                        }
                    )
        except Exception as ex:
            print("error occur compareing faces + %s" % ex)
        try:
            if (response['FaceMatches'][0]['Similarity'] > threshold):
                return i, response['FaceMatches'][0]['Similarity']
        except:
            pass
        #print(response)
    return -1, 0
