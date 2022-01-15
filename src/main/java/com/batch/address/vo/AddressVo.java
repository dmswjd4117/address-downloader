package com.spring.batch;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
public class AddressVo {

    // 주소관할읍면동코드
    private String townCode;

    // 시
    private String cityName;

    // 구
    private String cityCountryName;

    // 동
    private String townName;

    // 도로명 코드
    private String roadNameCode;

    // 도로명
    private String roadName;

    // 지하여부
    private String undergroundStatus;

    // 건물본번
    private Integer buildingNum;

    // 건물부번
    private Integer buildingSideNum;

    // 우편번호
    private String zipCode;

    // 건물관리번호 (pk)
    // 법정동코드(10) + 산여부(1) + 지번본번(4) + 지번부번(4) + 시스템번호(6)
    private String buildingManagementNum;

    // 시군구용건물명
    private String buildingNameForCity;

    // 건축물용도분류
    private String buildingUseClassification;

    // 행정동코드
    private String administrativeTownCode;

    // 행정동명
    private String administrativeTownName;

    // 지상층수
    private Integer groundFloorNumber;

    // 지하층수
    private Integer undergroundFloorNumber;

    // 공동주택구분
    private String classificationApartmentBuildings;

    // 건물수
    private Integer buildingCnt;

    // 상세건물명
    private String detailBuildingName;

    // 건물명변경이력
    private String BuildingNameChangeHistory;

    // 상세건물명변경이력
    private String BuildingNameChangeHistoryDetail;

    // 거주여부
    private String livingStatus;

    // 건물중심점 x좌표
    private Double buildingCenterXCoordinate;

    // 건물중심점 y좌표
    private Double buildingCenterYCoordinate;

    // 출입구 x좌표
    private Double exitXCoordinate;

    // 출입구 y좌표
    private Double exitYCoordinate;

    // 시도명 영문
    private String cityNameEng;

    // 시군구명 영문
    private String cityCountryNameEng;

    // 읍면동명 영문
    private String townNameEng;

    // 도로명영문
    private String roadNameEng;

    // 읍면동구분
    private String townMobileClassification;

    // 이동사유코드
    private String mobileReasonCode;

    public AddressVo(){}
}
