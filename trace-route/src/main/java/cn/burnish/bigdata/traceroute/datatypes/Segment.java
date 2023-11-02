package cn.burnish.bigdata.traceroute.datatypes;

import lombok.*;

/**
 * 报文
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class Segment {
    public String vin; // Vin
    public Long obdTime;  //  时间
    public String series;  //  车系
    public Double mileage;  // 里程
    public Double oil; // 油量
    public Double power; // 电量
    public Double urea; // 尿素
    public Double speed; // 速度
    public String lng;  // 经度
    public String lat;  // 纬度
}