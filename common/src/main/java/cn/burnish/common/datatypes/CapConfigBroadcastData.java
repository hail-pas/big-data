package cn.burnish.common.datatypes;

import lombok.*;

import java.io.Serializable;
import java.util.HashMap;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString

public class CapConfigBroadcastData implements Serializable {
    private static final long serialVersionUID = 1L;
    public HashMap<String, SeriesStandData> seriesStandMap;
    public HashMap<Character, SAPPowerData> sapPowerMap;
    public HashMap<String, TruckData> truckMap;
}