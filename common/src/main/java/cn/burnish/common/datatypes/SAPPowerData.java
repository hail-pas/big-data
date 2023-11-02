package cn.burnish.common.datatypes;

import lombok.*;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class SAPPowerData {
    public Character sapCode;  //  sap代码
    public Double kwhCap; // 最大千瓦时
    public Double ahCap; // 最大安培
}
