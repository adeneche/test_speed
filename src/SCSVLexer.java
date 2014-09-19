// Generated from SCSV.g by ANTLR 4.4
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class SCSVLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.4", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		Comma=1, LineBreak=2, SimpleValue=3, QuotedValue=4;
	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] tokenNames = {
		"'\\u0000'", "'\\u0001'", "'\\u0002'", "'\\u0003'", "'\\u0004'"
	};
	public static final String[] ruleNames = {
		"Comma", "LineBreak", "SimpleValue", "QuotedValue"
	};


	public SCSVLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "SCSV.g"; }

	@Override
	public String[] getTokenNames() { return tokenNames; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\6)\b\1\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\3\2\3\2\7\2\16\n\2\f\2\16\2\21\13\2\3\3\5\3\24"+
		"\n\3\3\3\3\3\5\3\30\n\3\3\4\6\4\33\n\4\r\4\16\4\34\3\5\3\5\3\5\3\5\7\5"+
		"#\n\5\f\5\16\5&\13\5\3\5\3\5\2\2\6\3\3\5\4\7\5\t\6\3\2\4\6\2\f\f\17\17"+
		"$$..\3\2$$.\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\3\13\3\2\2"+
		"\2\5\27\3\2\2\2\7\32\3\2\2\2\t\36\3\2\2\2\13\17\7.\2\2\f\16\7\"\2\2\r"+
		"\f\3\2\2\2\16\21\3\2\2\2\17\r\3\2\2\2\17\20\3\2\2\2\20\4\3\2\2\2\21\17"+
		"\3\2\2\2\22\24\7\17\2\2\23\22\3\2\2\2\23\24\3\2\2\2\24\25\3\2\2\2\25\30"+
		"\7\f\2\2\26\30\7\17\2\2\27\23\3\2\2\2\27\26\3\2\2\2\30\6\3\2\2\2\31\33"+
		"\n\2\2\2\32\31\3\2\2\2\33\34\3\2\2\2\34\32\3\2\2\2\34\35\3\2\2\2\35\b"+
		"\3\2\2\2\36$\7$\2\2\37 \7$\2\2 #\7$\2\2!#\n\3\2\2\"\37\3\2\2\2\"!\3\2"+
		"\2\2#&\3\2\2\2$\"\3\2\2\2$%\3\2\2\2%\'\3\2\2\2&$\3\2\2\2\'(\7$\2\2(\n"+
		"\3\2\2\2\t\2\17\23\27\34\"$\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}