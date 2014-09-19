// Generated from SCSV.g by ANTLR 4.4
import org.antlr.v4.runtime.misc.NotNull;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link SCSVParser}.
 */
public interface SCSVListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link SCSVParser#file}.
	 * @param ctx the parse tree
	 */
	void enterFile(@NotNull SCSVParser.FileContext ctx);
	/**
	 * Exit a parse tree produced by {@link SCSVParser#file}.
	 * @param ctx the parse tree
	 */
	void exitFile(@NotNull SCSVParser.FileContext ctx);
	/**
	 * Enter a parse tree produced by {@link SCSVParser#value}.
	 * @param ctx the parse tree
	 */
	void enterValue(@NotNull SCSVParser.ValueContext ctx);
	/**
	 * Exit a parse tree produced by {@link SCSVParser#value}.
	 * @param ctx the parse tree
	 */
	void exitValue(@NotNull SCSVParser.ValueContext ctx);
	/**
	 * Enter a parse tree produced by {@link SCSVParser#row}.
	 * @param ctx the parse tree
	 */
	void enterRow(@NotNull SCSVParser.RowContext ctx);
	/**
	 * Exit a parse tree produced by {@link SCSVParser#row}.
	 * @param ctx the parse tree
	 */
	void exitRow(@NotNull SCSVParser.RowContext ctx);
}