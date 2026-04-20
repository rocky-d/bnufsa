import os
from dotenv import load_dotenv

class TradeParamsCalculator:
    def __init__(
        self, 
        long_op: float, # 多仓开仓价
        shrt_op: float, # 空仓开仓价
        isolatedWalletBalance: float, # 余额
        cumB: int, # 本合约维持保证金速算额
        MMR_B: float, # 维持保证金率
        ) -> None:
        if not load_dotenv():
            raise FileNotFoundError("Can not find .env file.")

        self.long_op: float = long_op
        self.shrt_op: float = shrt_op

        self._load_env_params()

        self._calculate_margin()
        self._calculate_average_opening_price()
        self._calculate_take_profit()
        self._calculate_stop_loss()

        self.calculate_liquidation_price(
            isolatedWalletBalance, 
            cumB, 
            MMR_B
        )

        self._calculate_mr()
        
    def _load_env_params(self) -> None:
        self.quantity: float = float(os.getenv("QUANTITY"))
        self.leverage: int = int(os.getenv("LEVERAGE"))
        self.cr_taker: float = float(os.getenv("CR_TAKER"))
        self.cr_maker: float = float(os.getenv("CR_MAKER"))
        self.risk: float = float(os.getenv("RISK"))
        self.volatility: float = float(os.getenv("VOLATILITY"))
        
        params = [
            self.quantity, self.leverage, self.cr_taker, self.cr_maker,
            self.risk, self.volatility
        ]
        if any(p is None for p in params):
            raise ValueError("one or more environment variables are not set.")

    # 保证金
    def _calculate_margin(self) -> None:
        self.long_mg: float = self.long_op * self.quantity / self.leverage
        self.shrt_mg: float = self.shrt_op * self.quantity / self.leverage

    # 平均开仓价
    def _calculate_average_opening_price(self) -> None:
        self.average_op: float = (self.long_op + self.shrt_op) / 2

    # 止盈价
    def _calculate_take_profit(self) -> None:
        self.long_tp: float = self.average_op + (self.average_op * self.volatility)
        self.shrt_tp: float = self.average_op - (self.average_op * self.volatility)

    # 止损价
    def _calculate_stop_loss(self) -> None:
        volatility_amount = self.average_op * self.volatility
        self.shrt_sl: float = self.long_tp - volatility_amount * self.risk
        self.long_sl: float = self.shrt_tp + volatility_amount * self.risk
        
    # 强平价
    def calculate_liquidation_price(
            self,
            isolatedWalletBalance: float, #余额
            cumB: float, #本合约维持保证金速算额
            MMR_B: float, #维持保证金率
            ) -> None:
        if isolatedWalletBalance is None or cumB is None or MMR_B is None:
            raise ValueError("one or more parameters are None")
        
        self.long_lq :float = max(0, (self.long_op * self.quantity - isolatedWalletBalance - cumB) / (self.quantity * (1 - MMR_B)))
        self.shrt_lq :float = max(0, (self.shrt_op * self.quantity + isolatedWalletBalance + cumB) / (self.quantity * (1 + MMR_B)))

        if self.long_lq >= self.long_tp or self.shrt_lq <= self.shrt_tp:
            raise ValueError("Liquidation price is not acceptable, trade is too risky.")
        
    # 边际效益
    def _calculate_mr(self) -> None:
        self.mr = self.volatility * (self.risk * (1 + self.cr_maker) - 2 * self.cr_maker) * self.average_op + (self.average_op * self.cr_maker * -2) - (self.long_op * (self.cr_taker + 1) + self.shrt_op * (self.cr_taker - 1))
        
        if self.mr <= 0:
            raise ValueError("Marginal Revenue (mr) is negative, trade is not profitable.")

    # 仓位价值
    def calculate_position_value(
        self, 
        mp:float # 标记价格
        ) -> float:
        if mp is None:
            raise ValueError("mp is None")
        return mp * self.quantity

    def __str__(self):
        return f"""
        开仓参数:
          - 多仓开仓价 (long_op): {self.long_op}
          - 空仓开仓价 (shrt_op): {self.shrt_op}

        自定义参数:
          - 数量 (quantity): {self.quantity}
          - 杠杆 (leverage): {self.leverage}
          - 风险系数 (risk): {self.risk}
          - 波动率 (volatility): {self.volatility}
          - Taker 费率 (cr_taker): {self.cr_taker}
          - Maker 费率 (cr_maker): {self.cr_maker}

        交易参数:
          - 平均开仓价 (average_op): {self.average_op:.6f}
          
          - 多仓保证金 (long_mg): {self.long_mg:.6f}
          - 空仓保证金 (shrt_mg): {self.shrt_mg:.6f}
          
          - 多仓止盈 (long_tp): {self.long_tp:.6f}
          - 多仓止损 (long_sl): {self.long_sl:.6f}
          - 多仓强平 (long_lq): {self.long_lq:.6f}
          
          - 空仓止盈 (shrt_tp): {self.shrt_tp:.6f}
          - 空仓止损 (shrt_sl): {self.shrt_sl:.6f}
          - 空仓强平 (shrt_lq): {self.shrt_lq:.6f}

          - 边际效益 (mr): {self.mr:.6f}

        """
        
    
if __name__ == "__main__":
    try:
        trade_session = TradeParamsCalculator(long_op=0.21322, shrt_op=0.21322, isolatedWalletBalance=2, cumB=0, MMR_B=0.0065)
        print(trade_session)

    except (FileNotFoundError, ValueError) as e:
        print(f"错误: {e}")