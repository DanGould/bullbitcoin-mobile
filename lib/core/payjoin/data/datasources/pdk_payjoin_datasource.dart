import 'dart:async';
import 'dart:convert';
import 'dart:developer';
import 'dart:isolate';
import 'dart:typed_data';

import 'package:bb_mobile/core/errors/bull_exception.dart';
import 'package:bb_mobile/core/payjoin/data/models/payjoin_input_pair_model.dart';
import 'package:bb_mobile/core/payjoin/data/models/payjoin_model.dart';
import 'package:bb_mobile/core/utils/constants.dart';
import 'package:bb_mobile/core/utils/logger.dart' as logger;
import 'package:bb_mobile/core/utils/transaction_parsing.dart';
import 'package:dio/dio.dart';
import 'package:payjoin/bitcoin.dart';
import 'package:payjoin/payjoin_ffi.dart';

class PdkPayjoinDatasource {
  final String _payjoinDirectoryUrl;
  final Dio _dio;
  final StreamController<PayjoinReceiverModel> _payjoinRequestedController;
  final StreamController<PayjoinSenderModel> _proposalSentController;
  final StreamController<PayjoinModel> _expiredController;

  // Background processing
  Isolate? _receiversIsolate;
  Isolate? _sendersIsolate;
  SendPort? _receiversIsolatePort;
  SendPort? _sendersIsolatePort;
  final Completer _receiversIsolateReady;
  final Completer _sendersIsolateReady;

  PdkPayjoinDatasource({
    String payjoinDirectoryUrl = PayjoinConstants.directoryUrl,
    required Dio dio,
  }) : _payjoinDirectoryUrl = payjoinDirectoryUrl,
       _dio = dio,
       _payjoinRequestedController = StreamController.broadcast(),
       _proposalSentController = StreamController.broadcast(),
       _expiredController = StreamController.broadcast(),
       _receiversIsolateReady = Completer(),
       _sendersIsolateReady = Completer();

  Stream<PayjoinReceiverModel> get requestsForReceivers =>
      _payjoinRequestedController.stream;

  Stream<PayjoinSenderModel> get proposalsForSenders =>
      _proposalSentController.stream;

  Stream<PayjoinModel> get expiredPayjoins => _expiredController.stream;

  Future<(OhttpKeys?, Url?)> fetchOhttpKeyAndRelay({
    required String payjoinDirectory,
  }) async {
    Url? ohttpRelay;
    OhttpKeys? ohttpKeys;
    for (final ohttpRelayUrl in PayjoinConstants.ohttpRelayUrls) {
      try {
        final relay = Url.parse(ohttpRelayUrl);
        ohttpKeys = await fetchOhttpKeys(ohttpRelayUrl, payjoinDirectory);
        ohttpRelay = relay;
        break;
      } catch (e) {
        continue;
      }
    }
    return (ohttpKeys, ohttpRelay);
  }

  Future<PayjoinReceiverModel> createReceiver({
    required String walletId,
    required String address,
    required bool isTestnet,
    required BigInt maxFeeRateSatPerVb,
    required int expireAfterSec,
  }) async {
    try {
      final (ohttpKeys, ohttpRelay) = await fetchOhttpKeyAndRelay(
        payjoinDirectory: _payjoinDirectoryUrl,
      );

      if (ohttpRelay == null || ohttpKeys == null) {
        throw Exception('All OHTTP relays failed');
      }

      final persister = ReceiverPersisterAdapter();
      final bitcoinAddress = Address(
        address,
        isTestnet ? Network.testnet : Network.bitcoin,
      );

      final initialReceiveTransition =
          ReceiverBuilder(
            bitcoinAddress,
            _payjoinDirectoryUrl,
            ohttpKeys,
          ).withExpiration(expireAfterSec).build();

      final initialized = initialReceiveTransition.save(persister);

      // Create and store the model to keep track of the payjoin session
      // TODO: Get proper ID from the session and ensure it's queryable later on
      final sessionId = DateTime.now().millisecondsSinceEpoch.toString();
      final model =
          PayjoinModel.receiver(
                id: sessionId,
                address: address,
                isTestnet: isTestnet,
                receiver: persister.toJson(),
                walletId: walletId,
                pjUri: initialized.pjUri().asString(),
                maxFeeRateSatPerVb: maxFeeRateSatPerVb,
                createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
                expireAfterSec: expireAfterSec,
              )
              as PayjoinReceiverModel;

      // Start listening for a payjoin request from the sender in an isolate
      await startListeningForRequest(model);

      return model;
    } catch (e) {
      throw ReceiveCreationException(e.toString());
    }
  }

  Future<PayjoinSenderModel> createSender({
    required String walletId,
    required bool isTestnet,
    required String bip21,
    required String originalPsbt,
    required int amountSat,
    required double networkFeesSatPerVb,
    int? expireAfterSec,
  }) async {
    final expirySec = expireAfterSec ?? PayjoinConstants.defaultExpireAfterSec;
    final uri = Uri.parse(bip21);

    PjUri pjUri;
    try {
      pjUri = uri.checkPjSupported();
    } catch (e) {
      throw NoValidPayjoinBip21Exception(e.toString());
    }

    final minFeeRateSatPerKwu = (networkFeesSatPerVb * 250).toInt();
    final persister = SenderPersisterAdapter();

    final initialSendTransition = SenderBuilder(
      originalPsbt,
      pjUri,
    ).buildRecommended(minFeeRateSatPerKwu);
    final withReplyKey = initialSendTransition.save(persister);

    // Create and store the model with the data needed to keep track of the
    //  payjoin session
    final model =
        PayjoinModel.sender(
              uri: uri.asString(),
              isTestnet: isTestnet,
              sender: persister.toJson(),
              walletId: walletId,
              originalPsbt: originalPsbt,
              originalTxId: await TransactionParsing.getTxIdFromPsbt(
                originalPsbt,
              ),
              amountSat: amountSat,
              createdAt: DateTime.now().millisecondsSinceEpoch ~/ 1000,
              expireAfterSec: expirySec,
            )
            as PayjoinSenderModel;

    // Start listening for a payjoin proposal from the receiver in an isolate
    await startListeningForProposal(model);

    return model;
  }

  Future<PayjoinReceiverModel> proposePayjoin({
    required PayjoinReceiverModel receiverModel,
    required FutureOr<bool> Function(Uint8List) hasOwnedInputs,
    required FutureOr<bool> Function(Uint8List) hasReceiverOutput,
    required List<PayjoinInputPairModel> inputPairs,
    required FutureOr<String> Function(String) processPsbt,
  }) async {
    final persister = ReceiverPersisterAdapter.fromJson(receiverModel.receiver);

    final replayResult = replayReceiverEventLog(persister);
    final receiveSession = replayResult.state();

    UncheckedOriginalPayload request;

    if (receiveSession is InitializedReceiveSession) {
      final result = await getRequest(
        receiver: receiveSession.inner,
        dio: _dio,
        persister: persister,
      );
      if (result == null) {
        throw Exception('No request found');
      }
      request = result;
    } else if (receiveSession is UncheckedOriginalPayloadReceiveSession) {
      request = receiveSession.inner;
    } else {
      throw Exception(
        'TODO handle session state: ${receiveSession.runtimeType}',
      );
    }

    final interactiveReceiver = request.assumeInteractiveReceiver().save(
      persister,
    );
    final inputsNotOwned = interactiveReceiver
        .checkInputsNotOwned(IsScriptOwnedCallback(hasOwnedInputs))
        .save(persister);
    final inputsNotSeen = inputsNotOwned
        .checkNoInputsSeenBefore(
          // Assume the wallet has not seen the inputs since it is an interactive wallet
          IsOutputKnownCallback((_) => false),
        )
        .save(persister);
    final receiverOutputs = inputsNotSeen
        .identifyReceiverOutputs(IsScriptOwnedCallback(hasReceiverOutput))
        .save(persister);
    final committedOutputs = receiverOutputs.commitOutputs().save(persister);

    final candidateInputs =
        inputPairs
            .map(
              (input) => InputPair(
                TxIn(
                  OutPoint(input.txId, input.vout),
                  Script(input.scriptSigRawOutputScript as Uint8List),
                  input.sequence,
                  input.witness,
                ),
                PsbtInput(
                  TxOut(
                    Amount.fromSat(input.value!.toInt()),
                    Script(input.scriptPubkey),
                  ),
                  input.redeemScriptRawOutputScript.isEmpty
                      ? null
                      : Script(input.redeemScriptRawOutputScript as Uint8List),
                  input.witnessScriptRawOutputScript.isEmpty
                      ? null
                      : Script(input.witnessScriptRawOutputScript as Uint8List),
                ),
                null,
              ),
            )
            .toList();

    // Try to select a privacy preserving input pair, else just stick with the
    //  first possible input pair.
    InputPair inputPair = candidateInputs.first;
    try {
      inputPair = committedOutputs.tryPreservingPrivacy(candidateInputs);
    } catch (e) {
      logger.log.severe(
        'Failed to preserve privacy: $e. Using first input pair.',
      );
    }

    final inputsContributed = committedOutputs.contributeInputs([inputPair]);
    final inputsCommitted = inputsContributed.commitInputs().save(persister);
    final feesApplied = inputsCommitted
        .applyFeeRange(null, receiverModel.maxFeeRateSatPerVb.toInt())
        .save(persister);
    final proposal = feesApplied
        .finalizeProposal(ProcessPsbtCallback(processPsbt))
        .save(persister);

    // Now that the request is processed and the proposal is ready, send it to
    //  the sender through the payjoin directory
    await _sendPayjoinProposal(proposal);

    // Update the model with the proposal psbt so it can be known a proposal has
    //  been sent
    final proposalPsbt = proposal.psbt();

    final updatedModel = receiverModel.copyWith(
      receiver: persister.toJson(),
      proposalPsbt: proposalPsbt,
      txId: await TransactionParsing.getTxIdFromPsbt(proposalPsbt),
    );

    logger.log.info(
      'Payjoin request processed and proposal sent: $proposalPsbt',
    );

    return updatedModel;
  }

  Future<void> startListeningForRequest(PayjoinReceiverModel payjoin) async {
    if (_receiversIsolate == null) {
      // Start the isolate if it is not running yet
      await _spawnReceiversIsolate();
    }
    await _receiversIsolateReady.future;
    _receiversIsolatePort?.send(payjoin.toJson());
  }

  Future<void> startListeningForProposal(PayjoinSenderModel payjoin) async {
    if (_sendersIsolate == null) {
      // Start the isolate if it is not running yet
      await _spawnSendersIsolate();
    }
    await _sendersIsolateReady.future;
    _sendersIsolatePort?.send(payjoin.toJson());
  }

  /// Starts the isolate to listen for payjoin requests.
  Future<void> _spawnReceiversIsolate() async {
    // Receive isolate
    final receivePort = ReceivePort();

    // Listen to messages from the receive isolate
    receivePort.listen((message) {
      if (message is SendPort) {
        _receiversIsolatePort = message;
        _receiversIsolateReady.complete();
      } else if (message is Map<String, dynamic>) {
        logger.log.info(
          'Received message of found payjoin request in main isolate: $message',
        );
        final model = PayjoinReceiverModel.fromJson(message);

        // Send the updated payjoin model to the higher repository layers so it
        //  can be stored locally and processed further
        if (model.isExpired) {
          _expiredController.add(model);
        } else {
          // If not expired, it means a request was received
          _payjoinRequestedController.add(model);
        }
      }
    });

    logger.log.info('Spawning receivers isolate');
    // Spawn the isolate
    _receiversIsolate = await Isolate.spawn(
      _receiversIsolateEntryPoint,
      receivePort.sendPort,
    );
  }

  /// Starts the isolate to request and listen for payjoin proposals.
  Future<void> _spawnSendersIsolate() async {
    // Senders isolate
    final receivePort = ReceivePort();

    // Listen for messages from the senders isolate
    receivePort.listen((message) {
      if (message is SendPort) {
        _sendersIsolatePort = message;
        _sendersIsolateReady.complete();
      } else if (message is Map<String, dynamic>) {
        final model = PayjoinSenderModel.fromJson(message);

        // Send the updated payjoin model to the higher repository layers for
        //  processing and notification to the user
        if (model.isExpired) {
          _expiredController.add(model);
        } else {
          // If not expired, it means a proposal was received
          _proposalSentController.add(model);
        }
      }
    });

    logger.log.info('Spawning senders isolate');
    _sendersIsolate = await Isolate.spawn(
      _sendersIsolateEntryPoint,
      receivePort.sendPort,
    );
  }

  static Future<void> _receiversIsolateEntryPoint(SendPort sendPort) async {
    log('[Receivers Isolate] Started _receiversIsolateEntryPoint');

    final receivePort = ReceivePort();
    sendPort.send(receivePort.sendPort);
    final dio = Dio();
    final requests = <String, Future<void>>{};

    // Listen for and register new receivers sent from the main isolate
    receivePort.listen((data) {
      log('[Receivers Isolate] Received data in receivers isolate: $data');
      final receiverModel = PayjoinReceiverModel.fromJson(
        data as Map<String, dynamic>,
      );
      final persister = ReceiverPersisterAdapter.fromJson(
        receiverModel.receiver,
      );
      final replayResult = replayReceiverEventLog(persister);
      final receiveSession = replayResult.state();
      if (receiveSession is! InitializedReceiveSession) {
        log(
          '[Receivers Isolate] Expected InitializedReceiveSession but got ${receiveSession.runtimeType}',
        );
        return;
      }
      final receiver = receiveSession.inner;

      // Start checking for a payjoin request from the sender periodically
      const interval = Duration(
        seconds: PayjoinConstants.directoryPollingInterval,
      );
      Timer.periodic(interval, (Timer timer) async {
        log('[Receivers Isolate] Checking for request in receivers isolate');
        try {
          final request = await getRequest(
            receiver: receiver,
            dio: dio,
            persister: persister,
          );
          if (request != null) {
            requests.putIfAbsent(receiver.pjUri().asString(), () async {
              log('[Receivers Isolate] Request found in receivers isolate');
              // The original tx bytes are needed in the main isolate for
              //  further processing so extract them here and pass them through
              //  the model
              final extractable = request.assumeInteractiveReceiver().save(
                persister,
              );
              final originalTxBytes =
                  extractable.extractTxToScheduleBroadcast();
              final originalTxId =
                  await TransactionParsing.getTxIdFromTransactionBytes(
                    originalTxBytes,
                  );
              final amountSat =
                  await TransactionParsing.getAmountReceivedFromTransactionBytes(
                    originalTxBytes,
                    address: receiverModel.address,
                    isTestnet: receiverModel.isTestnet,
                  );
              log(
                '[Receivers Isolate] Request original Tx ID: $originalTxId and amount: $amountSat',
              );
              final updatedModel = receiverModel.copyWith(
                receiver: persister.toJson(),
                originalTxBytes: originalTxBytes,
                originalTxId: originalTxId,
                amountSat: amountSat,
              );

              // Notify the main isolate so it can be processed further
              sendPort.send(updatedModel.toJson());

              // Cancel the timer since the request has been received
              log('[Receivers Isolate] cancelling timer in receivers isolate');
              timer.cancel();
              log('[Receivers Isolate] timer cancelled in receivers isolate');
            });
          } else {
            log(
              '[Receivers Isolate] No valid request found in receivers isolate',
            );
          }
        } catch (e) {
          log('[Receivers Isolate] periodic timer get request exception: $e');
          if (e is PayjoinExpiredException) {
            // If the request returns an expiry error, mark the receiver as
            //  expired and notify the main isolate so it stops polling
            final updatedModel = receiverModel.copyWith(isExpired: true);
            sendPort.send(updatedModel.toJson());
            timer.cancel();
          }
        }
      });
    });
  }

  static Future<void> _sendersIsolateEntryPoint(SendPort sendPort) async {
    log('[Senders Isolate] Started _sendersIsolateEntryPoint');

    final receivePort = ReceivePort();
    sendPort.send(receivePort.sendPort);

    final dio = Dio();
    // Listen for and register new receivers sent from the main isolate
    receivePort.listen((data) async {
      try {
        log('[Senders Isolate] Received data in senders isolate: $data');
        final senderModel = PayjoinSenderModel.fromJson(
          data as Map<String, dynamic>,
        );
        final persister = SenderPersisterAdapter.fromJson(senderModel.sender);

        final replayResult = replaySenderEventLog(persister);
        final sendSession = replayResult.state();

        if (sendSession is! WithReplyKeySendSession) {
          log(
            '[Senders Isolate] Expected WithReplyKeySendSession but got ${sendSession.runtimeType}',
          );
          return;
        }

        final sender = sendSession.inner;
        log('[Senders Isolate] Requesting payjoin...');
        final context = await PdkPayjoinDatasource.request(
          sender: sender,
          dio: dio,
          persister: persister,
        );
        log('[Senders Isolate] Payjoin requested.');

        // Periodically check for a proposal from the receiver
        Timer.periodic(
          const Duration(seconds: PayjoinConstants.directoryPollingInterval),
          (Timer timer) async {
            log('[Senders Isolate]Checking for proposal in senders isolate');
            try {
              final proposalPsbt = await PdkPayjoinDatasource.getProposalPsbt(
                context: context,
                dio: dio,
                persister: persister,
              );

              if (proposalPsbt != null) {
                log('[Senders Isolate] Proposal found in senders isolate');
                final psbtStr = proposalPsbt.serializeBase64();
                final txId = await TransactionParsing.getTxIdFromPsbt(psbtStr);
                // The proposal psbt is needed in the main isolate for
                //  further processing so send it through the model as well as
                //  its txId.
                final updatedModel = senderModel.copyWith(
                  proposalPsbt: psbtStr,
                  txId: txId,
                );

                // Notify the main isolate so the payjoin can be processed further
                sendPort.send(updatedModel.toJson());

                // Cancel the timer
                timer.cancel();
              }
            } catch (e) {
              log('[Senders Isolate] periodic timer exception: $e');
              if (e is PayjoinExpiredException) {
                // If the request returns an expiry error, mark the receiver as
                //  expired and notify the main isolate so it stops polling
                final updatedModel = senderModel.copyWith(isExpired: true);
                sendPort.send(updatedModel.toJson());
                timer.cancel();
              }
            }
          },
        );
      } catch (e) {
        log('[Senders Isolate] Error in listener: $e');
        // Optionally notify the main isolate of the failure
      }
    });
  }

  static Future<UncheckedOriginalPayload?> getRequest({
    required Initialized receiver,
    required Dio dio,
    required ReceiverPersisterAdapter persister,
  }) async {
    // The use of ffiError here is a hack, we should change it once payjoin-flutter
    //  exposes different exceptions for specific errors
    Object? ffiError;
    try {
      RequestResponse? request;
      for (final ohttpRelay in PayjoinConstants.ohttpRelayUrls) {
        try {
          request = receiver.createPollRequest(ohttpRelay);
          ffiError = null;
          log('receiver extractReq success');
          break;
        } catch (e) {
          log('receiver extractReq exception: $e');
          ffiError = e;
          continue;
        }
      }

      if (request == null) {
        if (ffiError != null) {
          throw ffiError;
        }
        throw PayjoinNotFoundException('No payjoin request found');
      }

      log('request != null');
      final (req, context) = (request.request, request.clientResponse);
      final ohttpResponse = await dio.post(
        req.url,
        data: req.body,
        options: Options(
          headers: {'Content-Type': req.contentType},
          responseType: ResponseType.bytes,
        ),
      );
      log('processing request...');
      final proposal = receiver
          .processResponse(ohttpResponse.data as Uint8List, context)
          .save(persister);
      log('request processed');
      if (proposal is ProgressInitializedTransitionOutcome) {
        return proposal.inner;
      } else {
        return null;
      }
    } catch (e) {
      log('getRequest exception: $e');
      if (e == ffiError) {
        // TODO: Check for the correct error.
        //  We just assume the error is an expired error for now.
        throw PayjoinExpiredException(
          'Payjoin receiver $receiver.id() expired',
        );
      }
      return null;
    }
  }

  Future<void> _sendPayjoinProposal(PayjoinProposal proposal) async {
    RequestResponse? request;
    for (final ohttpRelayUrl in PayjoinConstants.ohttpRelayUrls) {
      try {
        request = proposal.createPostRequest(ohttpRelayUrl);
        break;
      } catch (e) {
        log('proposal extractReq exception: $e with relay $ohttpRelayUrl');
        continue;
      }
    }
    if (request == null) {
      throw PayjoinNotFoundException('No payjoin proposal found');
    }

    final (req, ohttpCtx) = (request.request, request.clientResponse);
    final res = await _dio.post(
      req.url,
      data: req.body,
      options: Options(
        headers: {'Content-Type': req.contentType},
        responseType: ResponseType.bytes,
      ),
    );
    proposal.processResponse(res.data as Uint8List, ohttpCtx);
  }

  static Future<PollingForProposal> request({
    required WithReplyKey sender,
    required Dio dio,
    required SenderPersisterAdapter persister,
  }) async {
    RequestOhttpContext? result;

    for (final ohttpProxyUrl in PayjoinConstants.ohttpRelayUrls) {
      try {
        log(
          '[Senders Isolate] Extracting V2 request from sender with relay: $ohttpProxyUrl',
        );
        result = sender.createV2PostRequest(ohttpProxyUrl);
        break;
      } catch (e) {
        final msg = (e as dynamic).msg ?? e.toString();
        log('[Senders Isolate] request error: $msg');
        log('[Senders Isolate] Continuing to next OHTTP relay');
        continue;
      }
    }

    if (result == null) {
      log('[Senders Isolate] All OHTTP relays failed');
      throw Exception('All OHTTP relays failed');
    }

    final (req, context) = (result.request, result.ohttpCtx);

    log('[Senders Isolate] Sending V2 request to ${req.url}');
    final res = await dio.post(
      req.url,
      data: req.body,
      options: Options(
        headers: {'Content-Type': req.contentType},
        responseType: ResponseType.bytes,
      ),
    );
    log('[Senders Isolate] Received response from ${req.url}');

    final pollingForProposal = sender
        .processResponse(res.data as Uint8List, context)
        .save(persister);

    log(
      '[Senders Isolate] Processed response for V2 request: $pollingForProposal ',
    );

    return pollingForProposal;
  }

  static Future<Psbt?> getProposalPsbt({
    required PollingForProposal context,
    required Dio dio,
    required SenderPersisterAdapter persister,
  }) async {
    // The use of ffiError here is a hack, we should change it once payjoin-flutter
    //  exposes different exceptions for specific errors
    Object? ffiError;
    try {
      RequestOhttpContext? result;
      for (final ohttpRelay in PayjoinConstants.ohttpRelayUrls) {
        try {
          result = context.createPollRequest(ohttpRelay);
          ffiError = null;
          log(
            'context extract request success: $result with relay $ohttpRelay',
          );
          break;
        } catch (e) {
          log('context extract request exception: $e with relay $ohttpRelay');
          ffiError = e;
          continue;
        }
      }

      if (result == null) {
        if (ffiError != null) {
          throw ffiError;
        }
        throw Exception('All OHTTP relays failed');
      }

      final (req, reqCtx) = (result.request, result.ohttpCtx);

      final res = await dio.post(
        req.url,
        data: req.body,
        options: Options(
          headers: {'Content-Type': req.contentType},
          responseType: ResponseType.bytes,
        ),
      );

      final proposalPsbt = context
          .processResponse(res.data as Uint8List, reqCtx)
          .save(persister);

      if (proposalPsbt is ProgressPollingForProposalTransitionOutcome) {
        return proposalPsbt.inner;
      } else {
        return null;
      }
    } catch (e) {
      log('getProposalPsbt exception: $e');
      if (e == ffiError) {
        // TODO: Check for the correct error.
        //  We just assume the error is an expired error for now.
        throw PayjoinExpiredException('Payjoin sender expired');
      }
      return null;
    }
  }
}

/// Temporary adapters for payjoin receiver and sender session persistence.
///
/// This adapter bridges the gap between the old payjoin_flutter API (which required
/// full object serialization) and the new payjoin_dart API (which uses session events).
/// It implements JsonReceiverSessionPersister by storing session events in memory.
///
/// Events are also persisted as JSON in the existing PayjoinModel.receiver field
/// and database. This is a hack but allows upgrading the payjoin dependency without
/// any database schema changes.
class ReceiverPersisterAdapter implements JsonReceiverSessionPersister {
  final List<String> _events = [];

  @override
  void save(String event) {
    _events.add(event);
  }

  @override
  List<String> load() {
    return _events;
  }

  @override
  void close() {
    // No-op for now
  }

  String toJson() {
    return jsonEncode(_events);
  }

  static ReceiverPersisterAdapter fromJson(String json) {
    final adapter = ReceiverPersisterAdapter();
    final events = jsonDecode(json) as List<dynamic>;
    adapter._events.addAll(events.cast<String>());
    return adapter;
  }
}

class SenderPersisterAdapter implements JsonSenderSessionPersister {
  final List<String> _events = [];

  @override
  void save(String event) {
    _events.add(event);
  }

  @override
  List<String> load() {
    return _events;
  }

  @override
  void close() {
    // No-op for now
  }

  String toJson() {
    return jsonEncode(_events);
  }

  static SenderPersisterAdapter fromJson(String json) {
    final adapter = SenderPersisterAdapter();
    final events = jsonDecode(json) as List<dynamic>;
    adapter._events.addAll(events.cast<String>());
    return adapter;
  }
}

// These callback wrappers convert async functions to sync
class IsScriptOwnedCallback implements IsScriptOwned {
  final FutureOr<bool> Function(Uint8List) _callback;

  IsScriptOwnedCallback(this._callback);

  @override
  bool callback(Uint8List script) {
    final result = _callback(script);
    if (result is Future<bool>) {
      final completer = Completer<bool>();
      result.then(completer.complete).catchError(completer.completeError);
      while (!completer.isCompleted) {}
      return completer.future as bool;
    }
    return result;
  }
}

class IsOutputKnownCallback implements IsOutputKnown {
  final FutureOr<bool> Function(dynamic) _callback;

  IsOutputKnownCallback(this._callback);

  @override
  bool callback(OutPoint outpoint) {
    final result = _callback(outpoint);
    if (result is Future<bool>) {
      final completer = Completer<bool>();
      result.then(completer.complete).catchError(completer.completeError);
      while (!completer.isCompleted) {}
      return completer.future as bool;
    }
    return result;
  }
}

class ProcessPsbtCallback implements ProcessPsbt {
  final FutureOr<String> Function(String) _callback;

  ProcessPsbtCallback(this._callback);

  @override
  String callback(String psbt) {
    final result = _callback(psbt);
    if (result is Future<String>) {
      final completer = Completer<String>();
      result.then(completer.complete).catchError(completer.completeError);
      while (!completer.isCompleted) {}
      return completer.future as String;
    }
    return result;
  }
}

class PayjoinNotFoundException extends BullException {
  PayjoinNotFoundException(super.message);
}

class ReceiveCreationException extends BullException {
  ReceiveCreationException(super.message);
}

class NoValidPayjoinBip21Exception extends BullException {
  NoValidPayjoinBip21Exception(super.message);
}

class PayjoinExpiredException extends BullException {
  PayjoinExpiredException(super.message);
}

class OhttpRelaysUnavailableException extends BullException {
  OhttpRelaysUnavailableException(super.message);
}
