<div align="center">
  <h1> AFI 데이터 엔지니어링 Project </h1>
  AWS를 이용한 실시간 데이터 압축 및 복원 파이프라인 구성
  <br><br>
  <img src="https://user-images.githubusercontent.com/86868063/151973874-c62feea1-6944-4d69-8fd3-e3b1ae58c489.png">
  <br><br>
  
  ### 👤 팀원 소개
  |김석재|김예린|노지훈|
  |:---:|:---:|:---:|
  |<img src="https://avatars.githubusercontent.com/u/86823305?v=4" width="100"/> | <img src="https://avatars.githubusercontent.com/u/86868063?v=4" width="100"/> | <img src="https://avatars.githubusercontent.com/u/86717381?v=4" width="100"/> |
  |[Github](https://github.com/Cloudblack)|[Github](https://github.com/yello-ow)|[Github](https://github.com/nojihun)|
  <br>
  
   ### ⚒ 기술 스택
  ![Python](https://img.shields.io/badge/Python-3766AB?style=flat-square&logo=Python&logoColor=FFFFFF) ![AWS](https://img.shields.io/badge/Amazon_aws-232F3E?style=flat-sqare&logo=Amazon+AWS&logoColor=white) ![S3](https://img.shields.io/badge/Amazon_S3-569A31?style=flat-square&logo=Amazon+S3&logoColor=FFFFFF) ![Lambda](https://img.shields.io/badge/Lambda-FF9900?style=flat-square&logo=AWS+Lambda&logoColor=white)
  <br><br>
  
</div> 

  ## 프로젝트 목표 
  - 로그 데이터의 규모가 커서 데이터 ETL 파이프라인 구축이 어려운 문제 해결 
  - 로그 데이터의 일부만 선별하여 인코딩 및 압축 후 다시 원본으로 복구 
  - ML/DL 연구를 위한 데이터 ETL 파이프라인 구축
  <br>
  
  ## 데이터 파이프라인 
  <img src="https://user-images.githubusercontent.com/86868063/152541959-fc00237e-9bc4-4591-8c6c-177ceec3b86d.png">
  <br>
  
  ## 프로젝트 과정 
  - Local 환경에서 api로 받은 데이터를 kinesis를 이용해 수집, 인코딩 및 압축 후 S3에 저장 
  - 인코딩 및 압축 과정에서 형식이 다른 6개의 데이터를 각각 다른 방법으로 인코딩 및 압축 진행 
  - Glue crawler를 이용해 S3에 저장된 데이터를 Glue data catalog로 만들어 줌 
  - API로 ML 연구를 위한 데이터를 요청하면 Lambda를 통해 쿼리, 압축해제 후 디코딩 된 데이터 반환
  <br>
  
  ## 프로젝트 결과
  - AWS의 다양한 서비스를 이용해 데이터 파이프라인을 구축
  - 데이터를 인코딩 및 압축하는 과정에서 데이터 용량 90% 축소
  <br>
  
  ## 프로젝트 회고
  - 프로젝트 기간동안 AWS의 다양한 서비스를 이용해 데이터 파이프라인을 구축함으로써 클라우드 시스템에 대해 이해할 수 있는 계기가 되었으나,   
    처음 이용하다보니 서비스 사용과 서비스간 연결에 대해 공부가 부족한 것이 느껴져 AWS에 대해 추가로 더 공부해보고자 하는 생각이 들었다. 
  
  
  
  
  
  

 
  
  <br>
  
 
  <br>

