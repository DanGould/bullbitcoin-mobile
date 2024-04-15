import 'package:bb_mobile/_model/transaction.dart';
import 'package:freezed_annotation/freezed_annotation.dart';

part 'swap_state.freezed.dart';

@freezed
class SwapState with _$SwapState {
  const factory SwapState({
    @Default(false) bool creatingInvoice,
    @Default('') String errCreatingInvoice,
    @Default('') String errCreatingSwapInv,
    @Default(false) bool generatingSwapInv,
    SwapTx? swapTx,
    Invoice? invoice,
    @Default(false) bool errSmallAmt,
    int? errHighFees,
  }) = _SwapState;
  const SwapState._();

  bool showWarning() => errSmallAmt || errHighFees != null;
}
