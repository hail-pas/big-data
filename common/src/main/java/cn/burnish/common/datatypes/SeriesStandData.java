package cn.burnish.common.datatypes;

import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class SeriesStandData {
    public String series;  //  车系
    public Double maxOil; // 最大油量
    public Double maxUrea; // 最大尿素
}
