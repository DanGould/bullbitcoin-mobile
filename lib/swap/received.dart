import 'package:bb_mobile/_model/transaction.dart';
import 'package:bb_mobile/_ui/app_bar.dart';
import 'package:bb_mobile/_ui/components/button.dart';
import 'package:bb_mobile/_ui/components/text.dart';
import 'package:bb_mobile/currency/bloc/currency_cubit.dart';
import 'package:bb_mobile/home/bloc/home_cubit.dart';
import 'package:bb_mobile/swap/bloc/watchtxs_bloc.dart';
import 'package:bb_mobile/swap/bloc/watchtxs_event.dart';
import 'package:bb_mobile/swap/bloc/watchtxs_state.dart';
import 'package:bb_mobile/wallet/bloc/event.dart';
import 'package:flutter/material.dart';
import 'package:flutter_animate/flutter_animate.dart';
import 'package:flutter_bloc/flutter_bloc.dart';
import 'package:font_awesome_flutter/font_awesome_flutter.dart';
import 'package:gap/gap.dart';
import 'package:oktoast/oktoast.dart';

class SwapAppListener extends StatelessWidget {
  const SwapAppListener({super.key, required this.child});

  final Widget child;

  @override
  Widget build(BuildContext context) {
    return MultiBlocListener(
      listeners: [
        BlocListener<WatchTxsBloc, WatchTxsState>(
          listenWhen: (previous, current) => previous.txPaid != current.txPaid,
          listener: (context, state) {
            if (state.txPaid == null) return;
            if (state.syncWallet != null) return;

            final tx = state.txPaid!;
            final amt = tx.outAmount;
            final amtStr = context.select((CurrencyCubit _) => _.state.getAmountInUnits(amt));
            final prefix = tx.actionPrefixStr();

            showToastWidget(
              position: ToastPosition.top,
              _AlertUI(text: '$prefix $amtStr'),
            );

            context.read<WatchTxsBloc>().add(ClearAlerts());
          },
        ),
        BlocListener<WatchTxsBloc, WatchTxsState>(
          listenWhen: (previous, current) => previous.syncWallet != current.syncWallet,
          listener: (context, state) {
            if (state.syncWallet == null) return;

            final tx = state.txPaid!;
            final amt = tx.outAmount;
            final amtStr = context.select((CurrencyCubit _) => _.state.getAmountInUnits(amt));
            final prefix = tx.actionPrefixStr();

            showToastWidget(
              position: ToastPosition.top,
              _AlertUI(text: '$prefix $amtStr'),
            );

            context
                .read<HomeCubit>()
                .state
                .getWalletBloc(
                  state.syncWallet!,
                )
                ?.add(SyncWallet());

            context.read<WatchTxsBloc>().add(ClearAlerts());
          },
        ),
      ],
      child: child,
    );
  }
}

class _AlertUI extends StatelessWidget {
  const _AlertUI({required this.text, this.onTap});

  final String text;
  final Function? onTap;

  @override
  Widget build(BuildContext context) {
    return Container(
      margin: const EdgeInsets.only(top: 8),
      padding: const EdgeInsets.all(16),
      color: Colors.green,
      child: Row(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          const Icon(FontAwesomeIcons.circleCheck),
          const Gap(8),
          BBText.body(text),
          const Spacer(),
          if (onTap != null)
            BBButton.text(
              label: 'View',
              onPressed: onTap!,
            ),
          const Gap(8),
        ],
      ),
    );
  }
}

class ReceiveSwapPaidSuccessPage extends StatelessWidget {
  const ReceiveSwapPaidSuccessPage({super.key, required this.tx});

  final SwapTx tx;

  @override
  Widget build(BuildContext context) {
    final amt = tx.outAmount;
    final amtStr = context.select((CurrencyCubit _) => _.state.getAmountInUnits(amt));
    return Scaffold(
      appBar: AppBar(flexibleSpace: const BBAppBar(text: 'Swap Received')),
      body: Column(
        mainAxisAlignment: MainAxisAlignment.center,
        children: [
          const BBText.body('Payment received'),
          const Gap(16),
          const Center(
            child: SizedBox(
              height: 200,
              width: 200,
              child: Icon(
                FontAwesomeIcons.circleCheck,
                color: Colors.green,
              ),
            ),
          ).animate().scale(),
          const Gap(16),
          BBText.body(amtStr),
        ],
      ),
    );
  }
}
