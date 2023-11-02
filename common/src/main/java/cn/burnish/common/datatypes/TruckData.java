package cn.burnish.common.datatypes;

import lombok.*;

import java.io.Serializable;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class TruckData implements Serializable {
    private static final long serialVersionUID = 1L;
    public Integer ID;  //  id
    public Integer Company;  // 企业id
    public Integer Team;  // 车队id
    public String Vin;  // 车架号
}
